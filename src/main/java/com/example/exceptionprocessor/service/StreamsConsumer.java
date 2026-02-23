package com.example.exceptionprocessor.service;

import com.example.exceptionprocessor.config.AppProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private static final int READ_COUNT = 16;
    private static final long MAX_RECLAIM_BATCH = 64;

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

    @PostConstruct
    public void start() {
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
        String stream = props.getStreams().getStreamName();
        String group = props.getStreams().getGroupName();

        while (running.get()) {
            try {
                Consumer consumer = Consumer.from(group, consumerName);
                StreamReadOptions options = StreamReadOptions.empty()
                        .count(READ_COUNT)
                        .block(Duration.ofSeconds(5));

                StreamOffset<String> offset = StreamOffset.create(stream, ReadOffset.lastConsumed());
                List<MapRecord<String, Object, Object>> records = redis.opsForStream().read(consumer, options, offset);

                if (records == null || records.isEmpty()) {
                    continue;
                }

                for (MapRecord<String, Object, Object> rec : records) {
                    handleRecord(group, rec);
                }
            } catch (Exception e) {
                log.warn("Stream poll error; continuing", e);
            }
        }
    }

    private void handleRecord(String group, MapRecord<String, Object, Object> rec) {
        String securityId = Objects.toString(rec.getValue().get("securityId"), null);
        if (securityId == null || securityId.isBlank()) {
            log.warn("Missing securityId: {}", rec);
            acknowledge(group, rec);
            return;
        }

        CompletableFuture<Void> future;
        try {
            future = processingService.publishAllBySecurityIdAsync(securityId);
        } catch (Exception ex) {
            log.error(
                    "Failed to submit async task securityId={} msgId={}",
                    securityId,
                    rec.getId(),
                    ex
            );
            return;
        }

        future.whenComplete(
                (v, ex) -> {
                    if (ex == null) {
                        acknowledge(group, rec);
                        return;
                    }

                    log.error(
                            "Async processing failed for securityId={} msgId={}",
                            securityId,
                            rec.getId(),
                            ex
                    );
                }
        );
    }

    private void acknowledge(String group, MapRecord<String, Object, Object> rec) {
        try {
            redis.opsForStream().acknowledge(group, rec);
        } catch (Exception e) {
            log.debug("ACK failed for {}: {}", rec.getId(), e.getMessage());
        }
    }

    @Scheduled(fixedDelayString = "#{${app.retry.reclaimer-interval-ms}}")
    public void reclaimStale() {
        try {
            String stream = props.getStreams().getStreamName();
            String group = props.getStreams().getGroupName();
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

            List<MapRecord<String, Object, Object>> claimed = redis.opsForStream().claim(
                    stream,
                    group,
                    consumerName,
                    Duration.ofMillis(idleMs),
                    toClaim.toArray(new RecordId[0])
            );

            if (claimed == null || claimed.isEmpty()) {
                return;
            }

            for (MapRecord<String, Object, Object> rec : claimed) {
                handleRecord(group, rec);
            }
        } catch (Exception e) {
            log.debug("Reclaimer issue: {}", e.getMessage());
        }
    }
}
