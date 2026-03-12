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
    // Data access layer for exceptions table.
    private final ExceptionRecordRepository repo;
    // Kafka sender abstraction.
    private final KafkaPublisher publisher;
    // Runtime tuning knobs (chunk size, topic name, etc.).
    private final AppProperties props;

    /**
     * Starts processing asynchronously on the named thread pool.
     *
     *`@Async` means caller gets a `CompletableFuture` immediately, while work
     * runs on background `proc-*` threads from AsyncConfig.
     */
    @Async("exceptionProcessingTaskExecutor")
    public CompletableFuture<Set<String>> fetchAndPublishBySecurityIdsAsync(Collection<String> securityIds) {
        // Fast-return on empty input to avoid unnecessary thread work.
        if (securityIds == null || securityIds.isEmpty()) {
            // Return an already-completed future because there is nothing to process.
            return CompletableFuture.completedFuture(Collections.emptySet());
        }

        // Start a Stream pipeline over the incoming collection to normalize request data.
        Set<String> requested = securityIds.stream()
                // Drop nulls and blank strings so we never query DB with invalid business keys.
                .filter(id -> id != null && !id.isBlank())
                // Collect into LinkedHashSet:
                // - Set => remove duplicates
                // - LinkedHashSet => keep first-seen order for deterministic processing/logging.
                .collect(Collectors.toCollection(LinkedHashSet::new));
        // If all incoming IDs were invalid after filtering, there is still no work to do.
        if (requested.isEmpty()) {
            // Return empty success set; caller can treat this batch as no-op.
            return CompletableFuture.completedFuture(Collections.emptySet());
        }

        // Wrap synchronous internal result into a completed future for async API consistency.
        return CompletableFuture.completedFuture(fetchAndPublishBySecurityIdsInternal(requested));
    }

    /**
     * Core orchestrator for one logical request.
     *
     *Processes security IDs in chunks to keep DB queries bounded, while isolating
     * per-securityId failures so one bad key does not block the rest.
     */
    private Set<String> fetchAndPublishBySecurityIdsInternal(Set<String> securityIds) {
        // Chunking limits SQL `IN (...)` list size and keeps memory usage predictable.
        int chunkSize = Math.max(1, props.getBatch().getSecurityIdQueryChunkSize());
        // Tracks IDs that finished successfully so caller can ACK corresponding stream messages.
        Set<String> successful = new LinkedHashSet<>();
        // Observability metric: total rows published in this call.
        long totalSent = 0;

        // Loop over IDs in fixed-size windows so each DB query remains bounded.
        for (int from = 0; from < securityIds.size(); from += chunkSize) {
            // Build current window [from, from + chunkSize).
            List<String> chunkIds = securityIds.stream().skip(from).limit(chunkSize).toList();
            // Fetch only records not yet processed, oldest first.
            List<ExceptionRecord> chunkRecords = repo.findBySecurityIdInAndProcessedAtIsNullOrderByOccurredAtAsc(chunkIds);
            if (chunkRecords.isEmpty()) {
                // If DB has no pending rows for an ID, we still consider it successfully handled.
                successful.addAll(chunkIds);
                // Move to next chunk without trying publish path.
                continue;
            }

            // Group rows by securityId so failure on one key does not block other keys in the same chunk.
            Map<String, List<ExceptionRecord>> bySecurityId = chunkRecords.stream()
                    .collect(Collectors.groupingBy(ExceptionRecord::getSecurityId, Collectors.toList()));

            for (String securityId : chunkIds) {
                // Pull rows for this specific ID; default to empty list if DB returned none for that ID.
                List<ExceptionRecord> records = bySecurityId.getOrDefault(securityId, Collections.emptyList());

                try {
                    // Publish rows for this securityId and persist processedAt only for successful sends.
                    PublishOutcome outcome = publishAndMarkProcessed(records);
                    // Track this ID as fully successful only if every row publish succeeded.
                    if (outcome.allPublished()) {
                        successful.add(securityId);
                    } else {
                        // Leave securityId out of success set so Redis event is not ACKed.
                        log.error(
                                "Batch processing incomplete for securityId={}; published={} failed={}",
                                securityId,
                                outcome.publishedCount(),
                                outcome.failedCount()
                        );
                    }
                    // Count only rows that were actually emitted to Kafka successfully.
                    totalSent += outcome.publishedCount();
                } catch (Exception ex) {
                    // Partial-failure design: continue with other IDs and leave failed one pending for retry.
                    log.error("Batch processing failed for securityId={}", securityId, ex);
                }
            }
        }

        // Summary log gives high-level visibility into throughput and partial failures.
        log.info(
                "Published {} records across {} requested securityId(s); successfulIds={}",
                totalSent,
                securityIds.size(),
                successful.size()
        );
        // Returned set is consumed by StreamsConsumer to decide which Redis events to ACK.
        return successful;
    }

    /**
     * Publishes rows and marks only successfully published rows as processed.
     *
     *If any row fails, caller keeps the securityId unacked so Redis can retry.
     */

    // all records of 1 id
    private PublishOutcome publishAndMarkProcessed(List<ExceptionRecord> records) {
        // Guard clause for IDs that currently have no pending DB rows.
        if (records.isEmpty()) {
            return new PublishOutcome(true, 0, 0);
        }

        // Kick off all Kafka sends first.
        List<PublishAttempt> publishAttempts = new ArrayList<>(records.size());
        // Convert and enqueue each DB row for asynchronous Kafka publish.
        for (ExceptionRecord rec : records) {
            // Convert DB entity to DTO payload we publish to Kafka.
            ExceptionRecordDTO dto = ExceptionRecordDTO.builder()
                    // Copy primary key for downstream traceability/debugging.
                    .id(rec.getId())
                    // Copy source service name to preserve producer context.
                    .serviceName(rec.getServiceName())
                    // Copy severity so alerts/consumers can classify incidents.
                    .severity(rec.getSeverity())
                    // Copy human-readable exception message.
                    .message(rec.getMessage())
                    // Copy original occurrence time for timeline reconstruction.
                    .occurredAt(rec.getOccurredAt())
                    // Copy correlation id for distributed tracing joins.
                    .correlationId(rec.getCorrelationId())
                    // Copy securityId so downstream consumers can key business logic.
                    .securityId(rec.getSecurityId())
                    // Finalize immutable DTO instance from builder.
                    .build();

            // Submit one async Kafka send and retain record+future association.
            publishAttempts.add(new PublishAttempt(rec, publisher.publishAsync(props.getKafka().getTopic(), dto)));
        }

        // Wait for each publish and collect per-record outcomes.
        List<ExceptionRecord> published = new ArrayList<>(records.size());
        int failedCount = 0;
        for (PublishAttempt attempt : publishAttempts) {
            try {
                // Blocks until this record's publish either succeeds or fails.
                attempt.publishFuture().join();
                // Only successful rows are eligible for processedAt persistence.
                published.add(attempt.record());
            } catch (Exception ex) {
                // Keep failed rows unprocessed so they are retried on next pass.
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
            // Mark successful rows processed at one consistent timestamp.
            Instant now = Instant.now();
            for (ExceptionRecord rec : published) {
                rec.setProcessedAt(now);
            }
            // Persist idempotency marker so only remaining failed rows are retried.
            repo.saveAll(published);
        }

        // Report whether all rows succeeded and how many were published vs failed.
        return new PublishOutcome(failedCount == 0, published.size(), failedCount);
    }

    // Couples one DB row with its async publish future for per-record outcome handling.
    private record PublishAttempt(ExceptionRecord record, CompletableFuture<Void> publishFuture) {
    }

    // Summary used by caller to decide ACK behavior at securityId granularity.
    private record PublishOutcome(boolean allPublished, int publishedCount, int failedCount) {
    }
}
