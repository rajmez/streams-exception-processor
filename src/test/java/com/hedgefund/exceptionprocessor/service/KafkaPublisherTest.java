package com.hedgefund.exceptionprocessor.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.hedgefund.exceptionprocessor.dto.ExceptionRecordDTO;
import com.hedgefund.exceptionprocessor.persistence.Severity;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

/**
 * Unit tests for KafkaPublisher.
 *
 * Goal: prove that success and failure from KafkaTemplate are propagated correctly
 * to callers, because upstream ACK logic depends on future completion status.
 */
class KafkaPublisherTest {
    // Mocked Kafka client, so tests are deterministic and do not require a broker.
    private KafkaTemplate<String, ExceptionRecordDTO> kafkaTemplate;
    // Class under test.
    private KafkaPublisher publisher;

    @BeforeEach
    void setUp() {
        // Fresh mock and publisher per test method.
        kafkaTemplate = Mockito.mock(KafkaTemplate.class);
        publisher = new KafkaPublisher(kafkaTemplate);
    }

    @Test
    void publishAsyncCompletesWhenKafkaSendSucceeds() {
        // Payload used by publisher.
        ExceptionRecordDTO dto = dto();

        // Simulate broker acknowledgment metadata returned by Spring Kafka.
        ProducerRecord<String, ExceptionRecordDTO> producerRecord =
                new ProducerRecord<>("exception-records", "svc:SEC1", dto);
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("exception-records", 0),
                0,
                10,
                System.currentTimeMillis(),
                Long.valueOf(0L),
                0,
                0
        );
        // Wrap producer record + metadata into successful SendResult.
        SendResult<String, ExceptionRecordDTO> sendResult = new SendResult<>(producerRecord, metadata);

        // Configure mock to return successful future.
        when(kafkaTemplate.send(eq("exception-records"), any(), eq(dto)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        // Should not throw: success path resolves future cleanly.
        publisher.publishAsync("exception-records", dto).join();
    }

    @Test
    void publishAsyncFailsWhenKafkaSendFails() {
        ExceptionRecordDTO dto = dto();
        // Configure mock to simulate broker/client send failure.
        when(kafkaTemplate.send(eq("exception-records"), any(), eq(dto)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker error")));

        // Publisher should surface error as runtime exception to caller.
        assertThatThrownBy(() -> publisher.publishAsync("exception-records", dto).join())
                .isInstanceOf(RuntimeException.class);
    }

    private static ExceptionRecordDTO dto() {
        // Minimal payload needed for key generation and serialization path.
        return ExceptionRecordDTO.builder()
                .id(1L)
                .serviceName("svc")
                .severity(Severity.HIGH)
                .message("test")
                .occurredAt(Instant.now())
                .securityId("SEC1")
                .build();
    }
}
