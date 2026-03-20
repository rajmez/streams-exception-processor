package com.hedgefund.exceptionprocessor.service;

import com.hedgefund.exceptionprocessor.config.AppProperties;
import com.hedgefund.exceptionprocessor.persistence.ExceptionRecord;
import com.hedgefund.exceptionprocessor.dto.ExceptionRecordDTO;
import com.hedgefund.exceptionprocessor.repo.ExceptionRecordRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 * Business service that performs the "re-drive" workflow:
 * 1) fetch unprocessed DB exception rows by securityId,
 * 2) publish each row to Kafka,
 * 3) mark rows as processed only after successful publish.
 *
 *This is where idempotency is enforced using the `processedAt` column.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ExceptionProcessingService {
    private final ExceptionRecordRepository repo;
    private final KafkaPublisher publisher;
    private final AppProperties props;

    /**
     * Starts processing asynchronously on the named thread pool.
     *
     *`@Async` means caller gets a `CompletableFuture` immediately, while work
     * runs on background `proc-*` threads from AsyncConfig.
     */
    @Async("exceptionProcessingTaskExecutor")
    public CompletableFuture<Set<String>> fetchAndPublishBySecurityIdsAsync(Collection<String> securityIds) {
        if (securityIds == null || securityIds.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptySet());
        }

        Set<String> requested = securityIds.stream()
                .filter(id -> id != null && !id.isBlank())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        if (requested.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptySet());
        }

        return CompletableFuture.completedFuture(fetchAndPublishBySecurityIdsInternal(requested));
    }

    /**
     * Core orchestrator for one logical request.
     *
     *Processes security IDs in chunks to keep DB queries bounded, while isolating
     * per-securityId failures so one bad key does not block the rest.
     */
    private Set<String> fetchAndPublishBySecurityIdsInternal(Set<String> securityIds) {
        int chunkSize = Math.max(1, props.getBatch().getSecurityIdQueryChunkSize());
        Set<String> successful = new LinkedHashSet<>();
        long totalSent = 0;

        for (int from = 0; from < securityIds.size(); from += chunkSize) {
            List<String> chunkIds = securityIds.stream().skip(from).limit(chunkSize).toList();
            List<ExceptionRecord> chunkRecords = repo.findBySecurityIdInAndProcessedAtIsNullOrderByOccurredAtAsc(chunkIds);
            if (chunkRecords.isEmpty()) {
                successful.addAll(chunkIds);
                continue;
            }

            Map<String, List<ExceptionRecord>> bySecurityId = chunkRecords.stream()
                    .collect(Collectors.groupingBy(ExceptionRecord::getSecurityId, Collectors.toList()));

            for (String securityId : chunkIds) {
                List<ExceptionRecord> records = bySecurityId.getOrDefault(securityId, Collections.emptyList());

                try {
                    PublishOutcome outcome = publishAndMarkProcessed(records);
                    if (outcome.allPublished()) {
                        successful.add(securityId);
                    } else {
                        log.error(
                                "Batch processing incomplete for securityId={}; published={} failed={}",
                                securityId,
                                outcome.publishedCount(),
                                outcome.failedCount()
                        );
                    }
                    totalSent += outcome.publishedCount();
                } catch (Exception ex) {
                    log.error("Batch processing failed for securityId={}", securityId, ex);
                }
            }
        }

        log.info(
                "Published {} records across {} requested securityId(s); successfulIds={}",
                totalSent,
                securityIds.size(),
                successful.size()
        );
        return successful;
    }

    /**
     * Publishes rows and marks only successfully published rows as processed.
     *
     *If any row fails, caller keeps the securityId unacked so Redis can retry.
     */

    private PublishOutcome publishAndMarkProcessed(List<ExceptionRecord> records) {
        if (records.isEmpty()) {
            return new PublishOutcome(true, 0, 0);
        }

        List<PublishAttempt> publishAttempts = new ArrayList<>(records.size());
        for (ExceptionRecord rec : records) {
            ExceptionRecordDTO dto = ExceptionRecordDTO.builder()
                    .id(rec.getId())
                    .serviceName(rec.getServiceName())
                    .severity(rec.getSeverity())
                    .message(rec.getMessage())
                    .occurredAt(rec.getOccurredAt())
                    .correlationId(rec.getCorrelationId())
                    .securityId(rec.getSecurityId())
                    .build();

            publishAttempts.add(new PublishAttempt(rec, publisher.publishAsync(props.getKafka().getTopic(), dto)));
        }

        List<ExceptionRecord> published = new ArrayList<>(records.size());
        int failedCount = 0;
        for (PublishAttempt attempt : publishAttempts) {
            try {
                attempt.publishFuture().join();
                published.add(attempt.record());
            } catch (Exception ex) {
                failedCount++;
                log.error(
                        "Kafka publish failed for exceptionRecordId={} securityId={}",
                        attempt.record().getId(),
                        attempt.record().getSecurityId(),
                        ex
                );
            }
        }

        if (!published.isEmpty()) {
            Instant now = Instant.now();
            for (ExceptionRecord rec : published) {
                rec.setProcessedAt(now);
            }
            repo.saveAll(published);
        }

        return new PublishOutcome(failedCount == 0, published.size(), failedCount);
    }

    private record PublishAttempt(ExceptionRecord record, CompletableFuture<Void> publishFuture) {
    }

    private record PublishOutcome(boolean allPublished, int publishedCount, int failedCount) {
    }
}
