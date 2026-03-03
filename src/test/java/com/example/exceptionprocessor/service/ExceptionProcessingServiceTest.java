package com.example.exceptionprocessor.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.exceptionprocessor.config.AppProperties;
import com.example.exceptionprocessor.persistence.ExceptionRecord;
import com.example.exceptionprocessor.persistence.Severity;
import com.example.exceptionprocessor.repo.ExceptionRecordRepository;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ExceptionProcessingServiceTest {
    private ExceptionRecordRepository repo;
    private KafkaPublisher publisher;
    private ExceptionProcessingService service;

    @BeforeEach
    void setUp() {
        repo = Mockito.mock(ExceptionRecordRepository.class);
        publisher = Mockito.mock(KafkaPublisher.class);

        AppProperties props = new AppProperties();
        props.getKafka().setTopic("exception-records");
        props.getBatch().setSecurityIdQueryChunkSize(100);

        service = new ExceptionProcessingService(repo, publisher, props);
    }

    @Test
    void publishesAndMarksProcessedForSuccessfulSecurityIds() {
        ExceptionRecord a1 = record(1L, "SEC_A");
        ExceptionRecord a2 = record(2L, "SEC_A");
        ExceptionRecord b1 = record(3L, "SEC_B");

        when(repo.findBySecurityIdInAndProcessedAtIsNullOrderByOccurredAtAsc(any()))
                .thenReturn(List.of(a1, a2, b1));
        when(publisher.publishAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        Set<String> result = service.publishBySecurityIdsAsync(List.of("SEC_A", "SEC_B")).join();

        assertThat(result).containsExactlyInAnyOrder("SEC_A", "SEC_B");
        assertThat(a1.getProcessedAt()).isNotNull();
        assertThat(a2.getProcessedAt()).isNotNull();
        assertThat(b1.getProcessedAt()).isNotNull();

        verify(publisher, times(3)).publishAsync(any(), any());
        verify(repo, times(2)).saveAll(any());
    }

    @Test
    void leavesFailedSecurityIdPendingAndContinuesOthers() {
        ExceptionRecord a1 = record(1L, "SEC_A");
        ExceptionRecord b1 = record(2L, "SEC_B");

        when(repo.findBySecurityIdInAndProcessedAtIsNullOrderByOccurredAtAsc(any()))
                .thenReturn(List.of(a1, b1));
        when(publisher.publishAsync(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("kafka down")));

        Set<String> result = service.publishBySecurityIdsAsync(List.of("SEC_A", "SEC_B")).join();

        assertThat(result).containsExactly("SEC_A");
        assertThat(a1.getProcessedAt()).isNotNull();
        assertThat(b1.getProcessedAt()).isNull();
        verify(repo, times(1)).saveAll(any());
    }

    @Test
    void treatsIdsWithNoRowsAsSuccessful() {
        when(repo.findBySecurityIdInAndProcessedAtIsNullOrderByOccurredAtAsc(any()))
                .thenReturn(List.of());

        Set<String> result = service.publishBySecurityIdsAsync(List.of("SEC_X")).join();

        assertThat(result).containsExactly("SEC_X");
        verify(publisher, times(0)).publishAsync(any(), any());
        verify(repo, times(0)).saveAll(any());
    }

    private static ExceptionRecord record(Long id, String securityId) {
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
