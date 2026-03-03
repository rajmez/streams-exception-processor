package com.example.exceptionprocessor.service;

import com.example.exceptionprocessor.config.AppProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
                t.setDaemon(true); //understand properly
                return t;
            }
    );

    private final String consumerName = UUID.randomUUID().toString();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Semaphore inFlightBatches = new Semaphore(1);

    @PostConstruct // post constructing StreamsConsumer bean that is
    public void start() {
        int maxInFlight = Math.max(1, props.getBatch().getMaxInFlightBatches());
        inFlightBatches.drainPermits();
        inFlightBatches.release(maxInFlight);
        poller.submit(this::pollLoop);
    }

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

    void handleBatch(String group, List<MapRecord<String, String, String>> records) {
        if (records == null || records.isEmpty()) {
            inFlightBatches.release();
            return;
        }

        Map<String, List<MapRecord<String, String, String>>> recordsBySecurityId = new LinkedHashMap<>();

        for (MapRecord<String, String, String> rec : records) {
            String securityId = Objects.toString(rec.getValue().get("securityId"), null);
            if (securityId == null || securityId.isBlank()) {
                log.warn("Missing securityId: {}", rec);
                acknowledge(group, rec);
                continue;
            }
            recordsBySecurityId.computeIfAbsent(securityId, k -> new ArrayList<>()).add(rec);
        }

        if (recordsBySecurityId.isEmpty()) {
            inFlightBatches.release();
            return;
        }

        Set<String> requestedIds = new LinkedHashSet<>(recordsBySecurityId.keySet());
        CompletableFuture<Set<String>> future;
        try {
            future = processingService.publishBySecurityIdsAsync(requestedIds);
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
                for (String successfulId : success) {
                    for (MapRecord<String, String, String> rec : recordsBySecurityId.getOrDefault(successfulId, Collections.emptyList())) {
                        acknowledge(group, rec);
                    }
                }

                Set<String> failedIds = requestedIds.stream().filter(id -> !success.contains(id)).collect(Collectors.toSet());
                if (!failedIds.isEmpty()) {
                    log.error("Batch processing incomplete; leaving {} securityId(s) pending for retry", failedIds.size());
                }
            } finally {
                inFlightBatches.release();
            }
        });
    }

    private void acknowledge(String group, MapRecord<String, String, String> rec) {
        try {
            redis.opsForStream().acknowledge(group, rec);
        } catch (Exception e) {
            log.debug("ACK failed for {}: {}", rec.getId(), e.getMessage());
        }
    }

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
                    toClaim.toArray(new RecordId[0])
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
