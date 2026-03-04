package com.example.exceptionprocessor.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.exceptionprocessor.config.AppProperties;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

@SuppressWarnings({"unchecked", "rawtypes"})
class StreamsConsumerTest {
    private StringRedisTemplate redis;
    private StreamOperations<String, Object, Object> streamOps;
    private ExceptionProcessingService processingService;
    private AppProperties props;
    private StreamsConsumer consumer;

    @BeforeEach
    void setUp() {
        redis = Mockito.mock(StringRedisTemplate.class);
        streamOps = Mockito.mock(StreamOperations.class);
        processingService = Mockito.mock(ExceptionProcessingService.class);
        props = new AppProperties();
        props.getStreams().setRedisStreamName("security.events");
        props.getStreams().setConsumerGroupName("exception-workers");
        props.getRetry().setClaimStaleAfterMs(60_000L);
        props.getBatch().setMaxInFlightBatches(4);

        when(redis.opsForStream()).thenReturn(streamOps);
        consumer = new StreamsConsumer(redis, processingService, props);
    }

    @Test
    void handleBatchAcknowledgesOnlySuccessfulSecurityIdsAndInvalidRecords() {
        MapRecord<String, String, String> secA = mockRecord("1-0", Map.of("securityId", "SEC_A"));
        MapRecord<String, String, String> secB = mockRecord("2-0", Map.of("securityId", "SEC_B"));
        MapRecord<String, String, String> invalid = mockRecord("3-0", Map.of("other", "x"));

        when(processingService.publishBySecurityIdsAsync(Set.of("SEC_A", "SEC_B")))
                .thenReturn(CompletableFuture.completedFuture(Set.of("SEC_A")));

        consumer.handleBatch("exception-workers", List.of(secA, secB, invalid));

        verify(streamOps, times(1)).acknowledge("exception-workers", invalid);
        verify(streamOps, times(1)).acknowledge("exception-workers", secA);
        verify(streamOps, never()).acknowledge("exception-workers", secB);
    }

    @Test
    void reclaimStaleClaimsAndProcessesEligibleMessages() {
        PendingMessagesSummary summary = Mockito.mock(PendingMessagesSummary.class);
        PendingMessages pending = Mockito.mock(PendingMessages.class);
        PendingMessage stale = Mockito.mock(PendingMessage.class);
        RecordId pendingId = RecordId.of("10-0");
        MapRecord<String, String, String> claimed = mockRecord("10-0", Map.of("securityId", "SEC_X"));

        when(summary.getTotalPendingMessages()).thenReturn(1L);
        when(streamOps.pending("security.events", "exception-workers")).thenReturn(summary);
        when(streamOps.pending(eq("security.events"), eq("exception-workers"), any(Range.class), eq(1L)))
                .thenReturn(pending);
        when(pending.isEmpty()).thenReturn(false);
        when(pending.size()).thenReturn(1);
        when(pending.iterator()).thenReturn(List.of(stale).iterator());
        when(stale.getElapsedTimeSinceLastDelivery()).thenReturn(Duration.ofMinutes(2));
        when(stale.getId()).thenReturn(pendingId);

        when(streamOps.claim(
                eq("security.events"),
                eq("exception-workers"),
                any(String.class),
                eq(Duration.ofMillis(60_000L)),
                any(RecordId[].class)
        )).thenReturn((List) List.of(claimed));

        when(processingService.publishBySecurityIdsAsync(Set.of("SEC_X")))
                .thenReturn(CompletableFuture.completedFuture(Set.of("SEC_X")));

        consumer.reclaimStale();

        verify(streamOps, times(1)).acknowledge("exception-workers", claimed);
    }

    @Test
    void handleBatchAcknowledgesDuplicateSecurityIdEventAndProcessesOnlyOneRequest() {
        MapRecord<String, String, String> first = mockRecord("1-0", Map.of("securityId", "SEC_A"));
        MapRecord<String, String, String> duplicate = mockRecord("2-0", Map.of("securityId", "SEC_A"));

        when(processingService.publishBySecurityIdsAsync(Set.of("SEC_A")))
                .thenReturn(CompletableFuture.completedFuture(Set.of("SEC_A")));

        consumer.handleBatch("exception-workers", List.of(first, duplicate));

        verify(streamOps, times(1)).acknowledge("exception-workers", duplicate);
        verify(streamOps, times(1)).acknowledge("exception-workers", first);
        verify(processingService, times(1)).publishBySecurityIdsAsync(Set.of("SEC_A"));
    }

    private static MapRecord<String, String, String> mockRecord(String id, Map<String, String> value) {
        MapRecord<String, String, String> record = Mockito.mock(MapRecord.class);
        when(record.getId()).thenReturn(RecordId.of(id));
        when(record.getValue()).thenReturn(value);
        return record;
    }
}
