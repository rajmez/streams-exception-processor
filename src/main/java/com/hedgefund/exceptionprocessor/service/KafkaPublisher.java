package com.hedgefund.exceptionprocessor.service;

import com.hedgefund.exceptionprocessor.dto.ExceptionRecordDTO;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

/**
 * Thin wrapper around Spring KafkaTemplate.
 *
 *Encapsulates key construction, logging, and error propagation so higher-level
 * services can treat "publish to Kafka" as one async operation.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaPublisher {
    private final KafkaTemplate<String, ExceptionRecordDTO> kafkaTemplate;

    /**
     * Sends one DTO to Kafka and returns completion state as a future.
     *
     *Caller uses this future to decide whether processing can be ACKed (success)
     * or must remain pending for retry (failure).
     */
    public CompletableFuture<Void> publishAsync(String topic, ExceptionRecordDTO dto) {
        String key = dto.getServiceName() + ":" + dto.getSecurityId();

        CompletableFuture<SendResult<String, ExceptionRecordDTO>> future = kafkaTemplate.send(topic, key, dto);

        return future.thenAccept(
                        res -> {
                            var md = res.getRecordMetadata();
                            log.info(
                                    "Published topic={} partition={} offset={} key={}",
                                    md.topic(),
                                    md.partition(),
                                    md.offset(),
                                    key
                            );
                        }
                )
                .exceptionally(
                        ex -> {
                            log.error("Kafka publish failed key={} topic={}", key, topic, ex);
                            throw new RuntimeException(ex);
                        }
                );
    }
}
