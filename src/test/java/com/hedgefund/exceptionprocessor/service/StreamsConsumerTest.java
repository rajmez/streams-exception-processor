package com.hedgefund.exceptionprocessor.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedgefund.exceptionprocessor.config.AppProperties;
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

/**
 * Unit tests for StreamsConsumer delivery semantics.
 *
 * These tests focus on correctness of ACK/retry behavior, which is the core
 * reliability contract of this service's Redis consumer-group integration.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
class StreamsConsumerTest {
    // Mocked Redis template.
    private StringRedisTemplate redis;
    // Mocked low-level stream operations used by consumer.
    private StreamOperations<String, Object, Object> streamOps;
    // Mocked business service so we can isolate delivery semantics.
    private ExceptionProcessingService processingService;
    // In-memory config object used by consumer.
    private AppProperties props;
    // Class under test.
    private StreamsConsumer consumer;

    @BeforeEach
    void setUp() {
        // Create mocks and config fresh for each test to avoid state bleed.
        redis = Mockito.mock(StringRedisTemplate.class);
        streamOps = Mockito.mock(StreamOperations.class);
        processingService = Mockito.mock(ExceptionProcessingService.class);
        props = new AppProperties();
        // Configure stream identity used by consumer.
        props.getStreams().setRedisStreamName("security.events");
        props.getStreams().setConsumerGroupName("exception-workers");
        // Configure stale threshold used by reclaim tests.
        props.getRetry().setClaimStaleAfterMs(60_000L);
        // Configure in-flight limits so handleBatch path has permits.
        props.getBatch().setMaxInFlightBatches(4);

        // Wire redis.opsForStream() to the mocked stream operations.
        when(redis.opsForStream()).thenReturn(streamOps);
        // Instantiate consumer directly without full Spring context.
        consumer = new StreamsConsumer(redis, processingService, props);
    }

    @Test
    void handleBatchAcknowledgesOnlySuccessfulSecurityIdsAndInvalidRecords() {
        // Valid record for SEC_A.
        MapRecord<String, String, String> secA = mockRecord("1-0", Map.of("securityId", "SEC_A"));
        // Valid record for SEC_B.
        MapRecord<String, String, String> secB = mockRecord("2-0", Map.of("securityId", "SEC_B"));
        // Invalid record without securityId field.
        MapRecord<String, String, String> invalid = mockRecord("3-0", Map.of("other", "x"));

        // Business service reports only SEC_A as successful.
        when(processingService.fetchAndPublishBySecurityIdsAsync(Set.of("SEC_A", "SEC_B")))
                .thenReturn(CompletableFuture.completedFuture(Set.of("SEC_A")));

        // Execute batch path directly.
        consumer.handleBatch("exception-workers", List.of(secA, secB, invalid));

        // Invalid record should be ACKed immediately (poison-message prevention).
        verify(streamOps, times(1)).acknowledge("exception-workers", invalid);
        // Successful ID should be ACKed after processing success.
        verify(streamOps, times(1)).acknowledge("exception-workers", secA);
        // Failed ID should remain pending (no ACK) so retry/reclaim can occur.
        verify(streamOps, never()).acknowledge("exception-workers", secB);
    }

    @Test
    void reclaimStaleClaimsAndProcessesEligibleMessages() {
        // Summary says there is one pending message.
        PendingMessagesSummary summary = Mockito.mock(PendingMessagesSummary.class);
        // Detailed pending collection placeholder.
        PendingMessages pending = Mockito.mock(PendingMessages.class);
        // Single pending message metadata.
        PendingMessage stale = Mockito.mock(PendingMessage.class);
        // Redis stream ID of pending entry.
        RecordId pendingId = RecordId.of("10-0");
        // Claimed record payload for business processing.
        MapRecord<String, String, String> claimed = mockRecord("10-0", Map.of("securityId", "SEC_X"));

        // Configure pending summary lookup.
        when(summary.getTotalPendingMessages()).thenReturn(1L);
        when(streamOps.pending("security.events", "exception-workers")).thenReturn(summary);
        // Configure pending detail lookup.
        when(streamOps.pending(eq("security.events"), eq("exception-workers"), any(Range.class), eq(1L)))
                .thenReturn(pending);
        when(pending.isEmpty()).thenReturn(false);
        when(pending.size()).thenReturn(1);
        when(pending.iterator()).thenReturn(List.of(stale).iterator());
        // Mark message as stale (> claim threshold).
        when(stale.getElapsedTimeSinceLastDelivery()).thenReturn(Duration.ofMinutes(2));
        when(stale.getId()).thenReturn(pendingId);

        // Configure claim call to return one claimed record.
        when(streamOps.claim(
                eq("security.events"),
                eq("exception-workers"),
                any(String.class),
                eq(Duration.ofMillis(60_000L)),
                any(RecordId[].class)
        )).thenReturn((List) List.of(claimed));

        // Processing reports claimed ID as successful.
        when(processingService.fetchAndPublishBySecurityIdsAsync(Set.of("SEC_X")))
                .thenReturn(CompletableFuture.completedFuture(Set.of("SEC_X")));

        // Trigger scheduled reclaim logic directly.
        consumer.reclaimStale();

        // Successful reclaimed record must be ACKed.
        verify(streamOps, times(1)).acknowledge("exception-workers", claimed);
    }

    @Test
    void handleBatchAcknowledgesDuplicateSecurityIdEventAndProcessesOnlyOneRequest() {
        // Two records with same ID simulate accidental duplicate event in one batch.
        MapRecord<String, String, String> first = mockRecord("1-0", Map.of("securityId", "SEC_A"));
        MapRecord<String, String, String> duplicate = mockRecord("2-0", Map.of("securityId", "SEC_A"));

        // Business layer called once with de-duplicated ID set.
        when(processingService.fetchAndPublishBySecurityIdsAsync(Set.of("SEC_A")))
                .thenReturn(CompletableFuture.completedFuture(Set.of("SEC_A")));

        // Execute batch path.
        consumer.handleBatch("exception-workers", List.of(first, duplicate));

        // Duplicate record should be ACKed as redundant.
        verify(streamOps, times(1)).acknowledge("exception-workers", duplicate);
        // Original successful record should also be ACKed.
        verify(streamOps, times(1)).acknowledge("exception-workers", first);
        // Processing should run only once for SEC_A.
        verify(processingService, times(1)).fetchAndPublishBySecurityIdsAsync(Set.of("SEC_A"));
    }

    private static MapRecord<String, String, String> mockRecord(String id, Map<String, String> value) {
        // Build lightweight mocked record instead of constructing real Redis record object.
        MapRecord<String, String, String> record = Mockito.mock(MapRecord.class);
        // Stub stream ID used in logs/ack paths.
        when(record.getId()).thenReturn(RecordId.of(id));
        // Stub field map payload used to read securityId.
        when(record.getValue()).thenReturn(value);
        return record;
    }
}
