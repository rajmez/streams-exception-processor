package com.example.exceptionprocessor.service;

import com.example.exceptionprocessor.config.AppProperties;
import com.example.exceptionprocessor.persistence.ExceptionRecord;
import com.example.exceptionprocessor.dto.ExceptionRecordDTO;
import com.example.exceptionprocessor.repo.ExceptionRecordRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class ExceptionProcessingService {
    private final ExceptionRecordRepository repo;
    private final KafkaPublisher publisher;
    private final AppProperties props;

    /**
     * Prevents duplicate concurrent processing for the same securityId within a single instance.
     * Redis consumer groups may still deliver duplicate or repeated messages; this avoids wasting
     * local worker capacity.
     */
    private final Set<String> inFlightSecurityIds = ConcurrentHashMap.newKeySet();

    @Async("exceptionProcessingTaskExecutor")
    public CompletableFuture<Set<String>> publishBySecurityIdsAsync(Collection<String> securityIds) {
        if (securityIds == null || securityIds.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptySet());
        }

        Set<String> requested = securityIds.stream()
                .filter(id -> id != null && !id.isBlank())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        if (requested.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptySet());
        }

        Set<String> accepted = requested.stream()
                .filter(inFlightSecurityIds::add)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<String> skipped = requested.stream()
                .filter(id -> !accepted.contains(id))
                .collect(Collectors.toSet());
        if (!skipped.isEmpty()) {
            log.debug("Skipping {} in-flight securityId(s)", skipped.size());
        }

        if (accepted.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptySet());
        }

        try {
            return CompletableFuture.completedFuture(publishBySecurityIdsInternal(accepted));
        } finally {
            inFlightSecurityIds.removeAll(accepted);
        }
    }

    private Set<String> publishBySecurityIdsInternal(Set<String> securityIds) {
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
                    publishAndMarkProcessed(records);
                    successful.add(securityId);
                    totalSent += records.size();
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

    private void publishAndMarkProcessed(List<ExceptionRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        List<CompletableFuture<Void>> publishFutures = new ArrayList<>(records.size());
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

            publishFutures.add(publisher.publishAsync(props.getKafka().getTopic(), dto));
        }

        CompletableFuture.allOf(publishFutures.toArray(new CompletableFuture[0])).join();

        Instant now = Instant.now();
        for (ExceptionRecord rec : records) {
            rec.setProcessedAt(now);
        }
        repo.saveAll(records);
    }
}
