package com.hedgefund.exceptionprocessor.service;
import com.hedgefund.exceptionprocessor.config.AppProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Redis stream ingestion boundary of the service.
 *
 *Big picture in this class:
 * 1) continuously read events from Redis consumer group,
 * 2) convert each event into a securityId processing request,
 * 3) delegate business work to {@link ExceptionProcessingService},
 * 4) ACK only successful events so failed ones stay pending for retry/reclaim.
 *
 *This class owns delivery semantics (read, ack, reclaim). It does not own
 * DB/Kafka business logic.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class StreamsConsumer {
    // Upper bound for how many stale pending messages to reclaim in one scheduled run.
    private static final long MAX_RECLAIM_BATCH = 64;
    // 5-second block timeout for Redis reads and also permit-acquire wait time.
    private static final Duration READ_BLOCK_TIMEOUT = Duration.ofSeconds(5);

    // Redis client used to read/ack/claim stream records.
    private final StringRedisTemplate redis;
    // Service that performs DB lookup + Kafka publishing logic asynchronously.
    private final ExceptionProcessingService processingService;
    // Externalized application properties (app.*).
    private final AppProperties props;

    // Single-thread poller so stream read loop runs in exactly one dedicated thread.
    private final ExecutorService poller = Executors.newSingleThreadExecutor(
            // Custom thread factory lambda; allows naming and daemon configuration.
            r -> {
                // Creates a named thread to make logs/thread dumps easier to read.
                Thread t = new Thread(r, "streams-poller");
                // Daemon thread does not keep JVM alive during shutdown.
                t.setDaemon(true);
                // Returns the configured thread instance to executor.
                return t;
            }
    );

    // Unique consumer name per application instance; used in Redis consumer group identity.
    private final String consumerName = UUID.randomUUID().toString();
    // Shared "keep running" flag checked by poll loop; atomic for thread-safe visibility.
    private final AtomicBoolean running = new AtomicBoolean(true);
    // Permit gate for max concurrent in-flight batches (backpressure control).
    private final Semaphore inFlightBatches = new Semaphore(1);

    /**
     * Starts the background poller thread after Spring creates this bean.
     *
     *`@PostConstruct` is a lifecycle hook in Jakarta/Spring that runs once
     * after dependency injection.
     */
    @PostConstruct
    public void start() {
        // Reads configured max in-flight batches; clamps minimum to 1 for safety.
        int maxInFlight = Math.max(1, props.getBatch().getMaxInFlightBatches());
        // Resets semaphore permits to 0 so we can reinitialize deterministically.
        inFlightBatches.drainPermits();
        // Sets runtime permit count to config value (backpressure capacity).
        inFlightBatches.release(maxInFlight);
        // Starts the infinite poll loop on dedicated executor thread.
        // We keep polling on a dedicated thread so Spring request threads are never blocked.
        poller.submit(this::pollLoop);
    }

    /**
     * Stops the poller gracefully during application shutdown.
     *
     *`@PreDestroy` is the matching lifecycle hook called before bean destruction.
     */
    @PreDestroy
    public void stop() {
        // Signals loop to stop on next iteration check.
        running.set(false);
        // Interrupts blocking calls and stops accepting new tasks.
        poller.shutdownNow();
        try {
            // Waits up to 5 seconds for poller thread to terminate.
            poller.awaitTermination(5, TimeUnit.SECONDS);
        // InterruptedException means this thread itself was interrupted while waiting.
        } catch (InterruptedException ignored) {
            // Restores interrupted status per Java best practice.
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Long-running consume loop.
     *
     *Uses Redis `XREADGROUP` semantics (via Spring APIs) to pull events for this
     * consumer instance and hand batches to {@link #handleBatch(String, List)}.
     */
    private void pollLoop() {
        // Resolves stream name once at loop start (configured in app properties).
        String stream = props.getStreams().getRedisStreamName();
        // Resolves consumer group name once at loop start.
        String group = props.getStreams().getConsumerGroupName();

        // Repeats until stop() flips running to false.
        while (running.get()) {
            try {
                // Redis consumer identity used by XREADGROUP (group + this instance consumer).
                Consumer consumer = Consumer.from(group, consumerName);
                // Build read options: bounded batch size + block for up to 5 seconds.
                StreamReadOptions options = StreamReadOptions.empty()
                        // Maximum records per read call (clamped to >= 1).
                        .count(Math.max(1, props.getBatch().getStreamReadCount()))
                        // Long-polling wait window to reduce busy looping.
                        .block(READ_BLOCK_TIMEOUT);

                // Read from last delivered/acknowledged position for this consumer group.
                StreamOffset<String> offset = StreamOffset.create(stream, ReadOffset.lastConsumed());
                // Suppresses generic cast warning due to Spring Redis API raw return shape.
                @SuppressWarnings("unchecked")
                // Performs blocking read from Redis Stream using consumer group semantics.
                List<MapRecord<String, String, String>> records =
                        (List<MapRecord<String, String, String>>) (List<?>) redis.opsForStream().read(consumer, options, offset);

                // No data arrived within block window; continue loop and read again.
                if (records == null || records.isEmpty()) {
                    continue;
                }

                // Acquire one in-flight permit; wait up to timeout, else skip this iteration.
                if (!inFlightBatches.tryAcquire(READ_BLOCK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    // Backpressure signal: processor is saturated, so avoid adding more work now.
                    log.debug("Backpressure: skipping poll iteration due to in-flight batch limit");
                    continue;
                }

                // Dispatch this batch; permit will be released when async processing completes.
                handleBatch(group, records);
            } catch (Exception e) {
                // Keep consumer alive on transient failures instead of crashing loop thread.
                log.warn("Stream poll error; continuing", e);
            }
        }
    }

    /**
     * Validates one Redis batch, submits async processing, and ACKs successful events.
     *
     *Important project behavior:
     * - invalid events are ACKed immediately (avoid poison-message loops),
     * - duplicates in same batch are ACKed as redundant input,
     * - failed IDs remain pending so reclaimer can retry.
     */
    void handleBatch(String group, List<MapRecord<String, String, String>> records) {
        // Defensive guard: if empty input, release permit so capacity is not leaked.
        if (records == null || records.isEmpty()) {
            // A permit was already acquired by caller before invoking handleBatch.
            inFlightBatches.release();
            return;
        }

        // Ordered unique set of requested IDs to pass to async processing service.
        Set<String> requestedIds = new LinkedHashSet<>();
        // Valid records to ACK later if processing for their securityId succeeds.
        List<ValidRecord> validRecords = new ArrayList<>(records.size());

        // Iterate every record in the batch to validate and collect unique securityIds.
        for (MapRecord<String, String, String> rec : records) {
            // Null-safe conversion of "securityId" field to String; null if missing.
            String securityId = Objects.toString(rec.getValue().get("securityId"), null);
            // Invalid messages cannot be processed; ACK to prevent poison-message retries.
            if (securityId == null || securityId.isBlank()) {
                log.warn("Missing securityId: {}", rec);
                acknowledge(group, rec);
                continue;
            }
            // Uniqueness is guaranteed upstream; ACK duplicates to avoid redundant work.
            if (!requestedIds.add(securityId)) {
                log.warn("Duplicate securityId in batch: {}; acknowledging duplicate event {}", securityId, rec.getId());
                acknowledge(group, rec);
                continue;
            }
            // Keep record+id pair so we can ACK the exact Redis record after async success.
            validRecords.add(new ValidRecord(rec, securityId));
        }

        // If nothing valid remained, release permit and return.
        if (validRecords.isEmpty()) {
            inFlightBatches.release();
            return;
        }

        // Future will eventually contain successful security IDs.
        CompletableFuture<Set<String>> future;
        try {
            // Submit async processing to worker executor configured in service.
            future = processingService.publishBySecurityIdsAsync(requestedIds);
        } catch (Exception ex) {
            // Submission failed before async start; release permit immediately.
            log.error("Failed to submit batch for {} securityId(s)", requestedIds.size(), ex);
            inFlightBatches.release();
            return;
        }

        // Completion callback runs on success or failure and always releases permit.
        future.whenComplete((successfulIds, ex) -> {
            try {
                // Async task failed; keep messages pending so reclaimer can retry later.
                if (ex != null) {
                    log.error("Batch processing failed for {} securityId(s)", requestedIds.size(), ex);
                    return;
                }

                // Treat null result defensively as empty success set.
                Set<String> success = successfulIds == null ? Collections.emptySet() : successfulIds;
                // ACK only records for IDs reported successful by processing service.
                for (ValidRecord validRecord : validRecords) {
                    if (success.contains(validRecord.securityId())) {
                        acknowledge(group, validRecord.record());
                    }
                }

                // We intentionally do not ACK failed IDs so Redis can redeliver via reclaim path.
                int failedCount = requestedIds.size() - success.size();
                if (failedCount > 0) {
                    log.error("Batch processing incomplete; leaving {} securityId(s) pending for retry", failedCount);
                }
            } finally {
                // Critical: always return permit even on exception to avoid deadlock/starvation.
                inFlightBatches.release();
            }
        });
    }

    private record ValidRecord(MapRecord<String, String, String> record, String securityId) {
    }

    /**
     * Sends Redis ACK for a single stream record.
     *
     *ACK removes the record from the group pending list; after ACK this message
     * is considered completed for this group.
     */
    private void acknowledge(String group, MapRecord<String, String, String> rec) {
        try {
            // Removes message from pending list for this group once handled successfully.
            redis.opsForStream().acknowledge(group, rec);
        } catch (Exception e) {
            // ACK failure is logged; message can be retried/reclaimed later.
            log.debug("ACK failed for {}: {}", rec.getId(), e.getMessage());
        }
    }

    /**
     * Scheduled recovery path for stuck pending messages.
     *
     *If a consumer crashes after reading but before ACK, Redis keeps entries in
     * PEL (pending entries list). This method periodically claims stale entries and
     * reprocesses them.
     */
    @Scheduled(fixedDelayString = "#{${app.retry.reclaimer-interval-ms}}")
    public void reclaimStale() {
        try {
            // Stream and group names from configuration.
            String stream = props.getStreams().getRedisStreamName();
            String group = props.getStreams().getConsumerGroupName();
            // Threshold: message considered stale if idle for at least this many ms.
            long idleMs = props.getRetry().getClaimStaleAfterMs();

            // Quick summary call to avoid heavier query when there are no pending messages.
            PendingMessagesSummary summary = redis.opsForStream().pending(stream, group);
            if (summary == null || summary.getTotalPendingMessages() == 0) {
                return;
            }

            // Reclaim at most MAX_RECLAIM_BATCH per run to keep scheduled job bounded.
            long count = Math.min(MAX_RECLAIM_BATCH, summary.getTotalPendingMessages());
            // Fetch pending entries metadata (IDs + idle time) in unbounded range.
            PendingMessages pending = redis.opsForStream().pending(stream, group, Range.unbounded(), count);
            if (pending == null || pending.isEmpty()) {
                return;
            }

            // Collect IDs of messages idle long enough to be considered stale.
            List<RecordId> toClaim = new ArrayList<>(pending.size());
            for (PendingMessage pm : pending) {
                // Only reclaim messages that exceeded idle threshold; this reduces claim churn.
                if (pm.getElapsedTimeSinceLastDelivery().toMillis() >= idleMs) {
                    toClaim.add(pm.getId());
                }
            }

            // Nothing eligible for reclaim in this run.
            if (toClaim.isEmpty()) {
                return;
            }

            // Suppresses generic cast warning around claim API return typing.
            @SuppressWarnings("unchecked")
            // Claim transfers ownership of stale messages to this consumer for retry.
            List<MapRecord<String, String, String>> claimed = (List<MapRecord<String, String, String>>) (List<?>) redis.opsForStream().claim(
                    stream,
                    group,
                    consumerName,
                    Duration.ofMillis(idleMs),
                    toClaim.toArray(new RecordId[toClaim.size()])
            );

            // Claim may race with other consumers; no claimed records means nothing to do.
            if (claimed == null || claimed.isEmpty()) {
                return;
            }

            // Non-blocking permit check: if saturated, skip reclaim work this time.
            if (!inFlightBatches.tryAcquire()) {
                log.debug("Skipping reclaim batch due to in-flight batch limit");
                return;
            }
            // Reuse same batch pipeline as normal poll path.
            handleBatch(group, claimed);
        } catch (Exception e) {
            // Keep scheduler resilient; log and continue next fixed-delay cycle.
            log.debug("Reclaimer issue: {}", e.getMessage());
        }
    }
}
