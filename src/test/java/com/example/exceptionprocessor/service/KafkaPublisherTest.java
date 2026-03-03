package com.example.exceptionprocessor.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.example.exceptionprocessor.dto.ExceptionRecordDTO;
import com.example.exceptionprocessor.persistence.Severity;
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

class KafkaPublisherTest {
    private KafkaTemplate<String, ExceptionRecordDTO> kafkaTemplate;
    private KafkaPublisher publisher;

    @BeforeEach
    void setUp() {
        kafkaTemplate = Mockito.mock(KafkaTemplate.class);
        publisher = new KafkaPublisher(kafkaTemplate);
    }

    @Test
    void publishAsyncCompletesWhenKafkaSendSucceeds() {
        ExceptionRecordDTO dto = dto();

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
        SendResult<String, ExceptionRecordDTO> sendResult = new SendResult<>(producerRecord, metadata);

        when(kafkaTemplate.send(eq("exception-records"), any(), eq(dto)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        publisher.publishAsync("exception-records", dto).join();
    }

    @Test
    void publishAsyncFailsWhenKafkaSendFails() {
        ExceptionRecordDTO dto = dto();
        when(kafkaTemplate.send(eq("exception-records"), any(), eq(dto)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker error")));

        assertThatThrownBy(() -> publisher.publishAsync("exception-records", dto).join())
                .isInstanceOf(RuntimeException.class);
    }

    private static ExceptionRecordDTO dto() {
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
