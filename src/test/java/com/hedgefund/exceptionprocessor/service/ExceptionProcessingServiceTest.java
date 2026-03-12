package com.hedgefund.exceptionprocessor.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedgefund.exceptionprocessor.config.AppProperties;
import com.hedgefund.exceptionprocessor.persistence.ExceptionRecord;
import com.hedgefund.exceptionprocessor.persistence.Severity;
import com.hedgefund.exceptionprocessor.repo.ExceptionRecordRepository;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Unit tests for ExceptionProcessingService business behavior.
 *
 * These tests validate project-critical guarantees:
 * 1) successful publish marks DB rows processed,
 * 2) partial failures do not block other securityIds,
 * 3) empty DB result is treated as successful no-op.
 */
class ExceptionProcessingServiceTest {
    // Mocked repository isolates business logic from real database.
    private ExceptionRecordRepository repo;
    // Mocked publisher isolates business logic from real Kafka.
    private KafkaPublisher publisher;
    // Service under test.
    private ExceptionProcessingService service;

    @BeforeEach
    void setUp() {
        // Create mocks fresh for every test to avoid cross-test state leakage.
        repo = Mockito.mock(ExceptionRecordRepository.class);
        publisher = Mockito.mock(KafkaPublisher.class);

        // Build minimal runtime config required by service logic.
        AppProperties props = new AppProperties();
        props.getKafka().setTopic("exception-records");
        props.getBatch().setSecurityIdQueryChunkSize(100);

        // Inject mocks + config into service under test.
        service = new ExceptionProcessingService(repo, publisher, props);
    }

    @Test
    void publishesAndMarksProcessedForSuccessfulSecurityIds() {
        // Create sample DB rows for two security IDs.
        ExceptionRecord a1 = record(1L, "SEC_A");
        ExceptionRecord a2 = record(2L, "SEC_A");
        ExceptionRecord b1 = record(3L, "SEC_B");

        // Repository returns all pending rows for requested IDs.
        when(repo.findBySecurityIdInAndProcessedAtIsNullOrderByOccurredAtAsc(any()))
                .thenReturn(List.of(a1, a2, b1));
        // Every Kafka send succeeds.
        when(publisher.publishAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        // Invoke async API and block in test using join().
        Set<String> result = service.fetchAndPublishBySecurityIdsAsync(List.of("SEC_A", "SEC_B")).join();

        // Both IDs should be reported as successful.
        assertThat(result).containsExactlyInAnyOrder("SEC_A", "SEC_B");
        // Every row should be marked processed on success.
        assertThat(a1.getProcessedAt()).isNotNull();
        assertThat(a2.getProcessedAt()).isNotNull();
        assertThat(b1.getProcessedAt()).isNotNull();

        // Verify one publish call per row.
        verify(publisher, times(3)).publishAsync(any(), any());
        // saveAll is called once per securityId group (SEC_A group + SEC_B group).
        verify(repo, times(2)).saveAll(any());
    }

    @Test
    void leavesFailedSecurityIdPendingAndContinuesOthers() {
        // One row for each ID so behavior is easy to observe.
        ExceptionRecord a1 = record(1L, "SEC_A");
        ExceptionRecord b1 = record(2L, "SEC_B");

        // Repository returns both rows.
        when(repo.findBySecurityIdInAndProcessedAtIsNullOrderByOccurredAtAsc(any()))
                .thenReturn(List.of(a1, b1));
        // First publish succeeds (SEC_A), second publish fails (SEC_B).
        when(publisher.publishAsync(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("kafka down")));

        // Service should continue processing even after one ID fails.
        Set<String> result = service.fetchAndPublishBySecurityIdsAsync(List.of("SEC_A", "SEC_B")).join();

        // Only successful ID is returned.
        assertThat(result).containsExactly("SEC_A");
        // Successful row gets processedAt.
        assertThat(a1.getProcessedAt()).isNotNull();
        // Failed row stays pending for retry/reclaim path.
        assertThat(b1.getProcessedAt()).isNull();
        // Only successful group is persisted.
        verify(repo, times(1)).saveAll(any());
    }

    @Test
    void marksOnlyPublishedRowsWhenOneRecordFailsWithinSameSecurityId() {
        // Two rows for the same ID simulate partial success within one securityId.
        ExceptionRecord a1 = record(1L, "SEC_A");
        ExceptionRecord a2 = record(2L, "SEC_A");

        when(repo.findBySecurityIdInAndProcessedAtIsNullOrderByOccurredAtAsc(any()))
                .thenReturn(List.of(a1, a2));
        // First publish succeeds, second fails.
        when(publisher.publishAsync(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("kafka down")));

        Set<String> result = service.fetchAndPublishBySecurityIdsAsync(List.of("SEC_A")).join();

        // Partial failure means securityId should stay pending (not ACKed by caller).
        assertThat(result).isEmpty();
        // Successfully published row is marked processed.
        assertThat(a1.getProcessedAt()).isNotNull();
        // Failed row remains pending for retry.
        assertThat(a2.getProcessedAt()).isNull();
        // Persist only successful rows.
        verify(repo, times(1)).saveAll(any());
    }

    @Test
    void treatsIdsWithNoRowsAsSuccessful() {
        // Repository returns no pending DB rows for requested ID.
        when(repo.findBySecurityIdInAndProcessedAtIsNullOrderByOccurredAtAsc(any()))
                .thenReturn(List.of());

        // No-row path should still count as logical success for ACK flow.
        Set<String> result = service.fetchAndPublishBySecurityIdsAsync(List.of("SEC_X")).join();

        assertThat(result).containsExactly("SEC_X");
        // No rows => no Kafka sends.
        verify(publisher, times(0)).publishAsync(any(), any());
        // No rows => nothing to persist.
        verify(repo, times(0)).saveAll(any());
    }

    private static ExceptionRecord record(Long id, String securityId) {
        // Helper builds minimal valid entity as if read from DB.
        return ExceptionRecord.builder()
                .id(id)
                .serviceName("svc")
                .severity(Severity.HIGH)
                .message("boom")
                .occurredAt(Instant.now())
                .securityId(securityId)
                .build();
    }
}
