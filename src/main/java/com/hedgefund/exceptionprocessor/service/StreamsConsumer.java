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
    private static final long MAX_RECLAIM_BATCH = 64;
    private static final Duration READ_BLOCK_TIMEOUT = Duration.ofSeconds(5);

    private final StringRedisTemplate redis;
    private final ExceptionProcessingService processingService;
    private final AppProperties props;

    private final ExecutorService poller = Executors.newSingleThreadExecutor(
            r -> {
                Thread t = new Thread(r, "streams-poller");
                t.setDaemon(true);
                return t;
            }
    );

    private final String consumerName = UUID.randomUUID().toString();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Semaphore inFlightBatches = new Semaphore(1);

    /**
     * Starts the background poller thread after Spring creates this bean.
     *
     *`@PostConstruct` is a lifecycle hook in Jakarta/Spring that runs once
     * after dependency injection.
     */
    @PostConstruct
    public void start() {
        int maxInFlight = Math.max(1, props.getBatch().getMaxInFlightBatches());
        inFlightBatches.drainPermits();
        inFlightBatches.release(maxInFlight);
        poller.submit(this::pollLoop);
    }

    /**
     * Stops the poller gracefully during application shutdown.
     *
     *`@PreDestroy` is the matching lifecycle hook called before bean destruction.
     */
    @PreDestroy
    public void stop() {
        running.set(false);
        poller.shutdownNow();
        try {
            poller.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
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
        String stream = props.getStreams().getRedisStreamName();
        String group = props.getStreams().getConsumerGroupName();

        while (running.get()) {
            try {
                Consumer consumer = Consumer.from(group, consumerName);
                StreamReadOptions options = StreamReadOptions.empty()
                        .count(Math.max(1, props.getBatch().getStreamReadCount()))
                        .block(READ_BLOCK_TIMEOUT);

                StreamOffset<String> offset = StreamOffset.create(stream, ReadOffset.lastConsumed());
                @SuppressWarnings("unchecked")
                List<MapRecord<String, String, String>> records =
                        (List<MapRecord<String, String, String>>) (List<?>) redis.opsForStream().read(consumer, options, offset);

                if (records == null || records.isEmpty()) {
                    continue;
                }

                if (!inFlightBatches.tryAcquire(READ_BLOCK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    log.debug("Backpressure: skipping poll iteration due to in-flight batch limit");
                    continue;
                }

                handleBatch(group, records);
            } catch (Exception e) {
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
        if (records == null || records.isEmpty()) {
            inFlightBatches.release();
            return;
        }

        Set<String> requestedIds = new LinkedHashSet<>();
        List<ValidRecord> validRecords = new ArrayList<>(records.size());

        for (MapRecord<String, String, String> rec : records) {
            String securityId = Objects.toString(rec.getValue().get("securityId"), null);
            if (securityId == null || securityId.isBlank()) {
                log.warn("Missing securityId: {}", rec);
                acknowledge(group, rec);
                continue;
            }
            if (!requestedIds.add(securityId)) {
                log.warn("Duplicate securityId in batch: {}; acknowledging duplicate event {}", securityId, rec.getId());
                acknowledge(group, rec);
                continue;
            }
            validRecords.add(new ValidRecord(rec, securityId));
        }

        if (validRecords.isEmpty()) {
            inFlightBatches.release();
            return;
        }

        CompletableFuture<Set<String>> future;
        try {
            future = processingService.fetchAndPublishBySecurityIdsAsync(requestedIds);
        } catch (Exception ex) {
            log.error("Failed to submit batch for {} securityId(s)", requestedIds.size(), ex);
            inFlightBatches.release();
            return;
        }

        future.whenComplete((successfulIds, ex) -> {
            try {
                if (ex != null) {
                    log.error("Batch processing failed for {} securityId(s)", requestedIds.size(), ex);
                    return;
                }

                Set<String> success = successfulIds == null ? Collections.emptySet() : successfulIds;
                for (ValidRecord validRecord : validRecords) {
                    if (success.contains(validRecord.securityId())) {
                        acknowledge(group, validRecord.record());
                    }
                }

                int failedCount = requestedIds.size() - success.size();
                if (failedCount > 0) {
                    log.error("Batch processing incomplete; leaving {} securityId(s) pending for retry", failedCount);
                }
            } finally {
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
            redis.opsForStream().acknowledge(group, rec);
        } catch (Exception e) {
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
            String stream = props.getStreams().getRedisStreamName();
            String group = props.getStreams().getConsumerGroupName();
            long idleMs = props.getRetry().getClaimStaleAfterMs();

            PendingMessagesSummary summary = redis.opsForStream().pending(stream, group);
            if (summary == null || summary.getTotalPendingMessages() == 0) {
                return;
            }

            long count = Math.min(MAX_RECLAIM_BATCH, summary.getTotalPendingMessages());
            PendingMessages pending = redis.opsForStream().pending(stream, group, Range.unbounded(), count);
            if (pending == null || pending.isEmpty()) {
                return;
            }

            List<RecordId> toClaim = new ArrayList<>(pending.size());
            for (PendingMessage pm : pending) {
                if (pm.getElapsedTimeSinceLastDelivery().toMillis() >= idleMs) {
                    toClaim.add(pm.getId());
                }
            }

            if (toClaim.isEmpty()) {
                return;
            }

            @SuppressWarnings("unchecked")
            List<MapRecord<String, String, String>> claimed = (List<MapRecord<String, String, String>>) (List<?>) redis.opsForStream().claim(
                    stream,
                    group,
                    consumerName,
                    Duration.ofMillis(idleMs),
                    toClaim.toArray(new RecordId[toClaim.size()])
            );

            if (claimed == null || claimed.isEmpty()) {
                return;
            }

            if (!inFlightBatches.tryAcquire()) {
                log.debug("Skipping reclaim batch due to in-flight batch limit");
                return;
            }
            handleBatch(group, claimed);
        } catch (Exception e) {
            log.debug("Reclaimer issue: {}", e.getMessage());
        }
    }
}
