package com.example.exceptionprocessor.service;
// Imports the strongly-typed configuration object that maps values from application.yml.
import com.example.exceptionprocessor.config.AppProperties;
// Spring lifecycle hook: method runs once after dependency injection is complete.
import jakarta.annotation.PostConstruct;
// Spring lifecycle hook: method runs right before bean destruction/shutdown.
import jakarta.annotation.PreDestroy;
// Represents time-based amounts (for example 5 seconds) in a type-safe way.
import java.time.Duration;
// Resizable array implementation of List; fast for append operations.
import java.util.ArrayList;
// Utility class with helpers like emptyList/emptySet for safe defaults.
import java.util.Collections;
// Set that preserves insertion order and removes duplicates.
import java.util.LinkedHashSet;
// List interface (ordered collection, allows duplicates).
import java.util.List;
// Map interface (key/value pairs).
import java.util.Map;
// Utility methods for null-safe object operations.
import java.util.Objects;
// Set interface (unique values, no duplicates).
import java.util.Set;
// Generates random UUIDs; used for unique consumer names.
import java.util.UUID;
// Future/promise type for async results and completion callbacks.
import java.util.concurrent.CompletableFuture;
// Interface for thread pools/executors.
import java.util.concurrent.ExecutorService;
// Factory methods to create ExecutorService implementations.
import java.util.concurrent.Executors;
// Concurrency primitive that controls access using permits (backpressure gate here).
import java.util.concurrent.Semaphore;
// Time units such as seconds/milliseconds for timeout APIs.
import java.util.concurrent.TimeUnit;
// Thread-safe boolean for stop/start flags shared across threads.
import java.util.concurrent.atomic.AtomicBoolean;
// Stream collectors such as toSet() used in stream pipelines.
import java.util.stream.Collectors;
// Lombok: generates constructor for final fields (dependency injection convenience).
import lombok.RequiredArgsConstructor;
// Lombok: generates logger field named "log".
import lombok.extern.slf4j.Slf4j;
// Spring Data range object (used for pending message queries).
import org.springframework.data.domain.Range;
// Redis Stream consumer identity (group + consumer name).
import org.springframework.data.redis.connection.stream.Consumer;
// Redis Stream record represented as map payload.
import org.springframework.data.redis.connection.stream.MapRecord;
// Metadata for one pending message in a Redis Stream group.
import org.springframework.data.redis.connection.stream.PendingMessage;
// Collection wrapper for multiple pending messages.
import org.springframework.data.redis.connection.stream.PendingMessages;
// Summary of pending state (total count, id range, etc.).
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
// Redis stream read offset marker (for example "last consumed").
import org.springframework.data.redis.connection.stream.ReadOffset;
// Redis stream record ID type.
import org.springframework.data.redis.connection.stream.RecordId;
// Pairs stream key with offset for read operations.
import org.springframework.data.redis.connection.stream.StreamOffset;
// Options for XREADGROUP, such as count and block timeout.
import org.springframework.data.redis.connection.stream.StreamReadOptions;
// Spring Redis template specialized for String keys/values.
import org.springframework.data.redis.core.StringRedisTemplate;
// Spring scheduler annotation to run method repeatedly with a fixed delay.
import org.springframework.scheduling.annotation.Scheduled;
// Marks this class as a Spring-managed service bean.
import org.springframework.stereotype.Service;

// Registers this class as a singleton bean in the Spring context.
@Service
// Lombok-generated constructor with all final fields (for constructor injection).
@RequiredArgsConstructor
// Lombok-generated logger: private static final Logger log = ...
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

    // Called automatically after bean construction and dependency injection.
    @PostConstruct
    public void start() {
        // Reads configured max in-flight batches; clamps minimum to 1 for safety.
        int maxInFlight = Math.max(1, props.getBatch().getMaxInFlightBatches());
        // Resets semaphore permits to 0 so we can reinitialize deterministically.
        inFlightBatches.drainPermits();
        // Sets runtime permit count to config value (backpressure capacity).
        inFlightBatches.release(maxInFlight);
        // Starts the infinite poll loop on dedicated executor thread.
        poller.submit(this::pollLoop);
    }

    // Called by Spring during shutdown to stop background worker cleanly.
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

    // Main consume loop: read records from Redis stream and dispatch batch processing.
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

    // Processes one Redis read batch and invokes async business logic.
    void handleBatch(String group, List<MapRecord<String, String, String>> records) {
        // Defensive guard: if empty input, release permit so capacity is not leaked.
        if (records == null || records.isEmpty()) {
            inFlightBatches.release();
            return;
        }

        // Valid records to ACK later if processing for their securityId succeeds.
        List<ValidRecord> validRecords = new ArrayList<>(records.size());
        // Ordered unique set of requested IDs to pass to async processing service.
        Set<String> requestedIds = new LinkedHashSet<>();

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

                // Anything not successful remains pending for retry/reclaim.
                Set<String> failedIds = requestedIds.stream().filter(id -> !success.contains(id)).collect(Collectors.toSet());
                if (!failedIds.isEmpty()) {
                    log.error("Batch processing incomplete; leaving {} securityId(s) pending for retry", failedIds.size());
                }
            } finally {
                // Critical: always return permit even on exception to avoid deadlock/starvation.
                inFlightBatches.release();
            }
        });
    }

    private record ValidRecord(MapRecord<String, String, String> record, String securityId) {
    }

    // ACK helper: acknowledges one stream record to the consumer group.
    private void acknowledge(String group, MapRecord<String, String, String> rec) {
        try {
            // Removes message from pending list for this group once handled successfully.
            redis.opsForStream().acknowledge(group, rec);
        } catch (Exception e) {
            // ACK failure is logged; message can be retried/reclaimed later.
            log.debug("ACK failed for {}: {}", rec.getId(), e.getMessage());
        }
    }

    // Scheduled reclaimer: periodically claims stale pending messages and retries them.
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
                    toClaim.toArray(new RecordId[0])
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
