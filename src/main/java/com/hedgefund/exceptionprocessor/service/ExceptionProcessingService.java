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
    // Data access layer for exception_record table.
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
    public CompletableFuture<Set<String>> publishBySecurityIdsAsync(Collection<String> securityIds) {
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
        return CompletableFuture.completedFuture(publishBySecurityIdsInternal(requested));
    }

    /**
     * Core orchestrator for one logical request.
     *
     *Processes security IDs in chunks to keep DB queries bounded, while isolating
     * per-securityId failures so one bad key does not block the rest.
     */
    private Set<String> publishBySecurityIdsInternal(Set<String> securityIds) {
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
                    // Publish all rows for this securityId and persist processedAt on success.
                    publishAndMarkProcessed(records);
                    // Track this ID as fully successful so caller can ACK matching Redis message.
                    successful.add(securityId);
                    // Count how many DB rows were emitted to Kafka for final log metric.
                    totalSent += records.size();
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
     * Publishes all rows in the provided list and marks them processed atomically in logic order.
     *
     *Design rule: "publish first, mark processed second". This avoids losing messages
     * if Kafka publish fails midway.
     */
    private void publishAndMarkProcessed(List<ExceptionRecord> records) {
        // Guard clause for IDs that currently have no pending DB rows.
        if (records.isEmpty()) {
            return;
        }

        // Kick off all Kafka sends first, then wait for all to complete.
        List<CompletableFuture<Void>> publishFutures = new ArrayList<>(records.size());
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

            // Submit one async Kafka send and keep its future for aggregate success check.
            publishFutures.add(publisher.publishAsync(props.getKafka().getTopic(), dto));
        }

        // `join()` throws if any send failed; this prevents premature processedAt updates.
        // allOf(...) creates a combined future that completes only when every publish future completes.
        CompletableFuture.allOf(publishFutures.toArray(new CompletableFuture[0])).join();

        // Mark all rows processed at one consistent timestamp after successful publishes.
        Instant now = Instant.now();
        // Apply the same processedAt value to every record in this batch for audit consistency.
        for (ExceptionRecord rec : records) {
            rec.setProcessedAt(now);
        }
        // Persist idempotency marker so these rows are skipped in later runs.
        repo.saveAll(records);
    }
}
