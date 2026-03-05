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
    // Spring-provided Kafka producer client abstraction.
    private final KafkaTemplate<String, ExceptionRecordDTO> kafkaTemplate;

    /**
     * Sends one DTO to Kafka and returns completion state as a future.
     *
     *Caller uses this future to decide whether processing can be ACKed (success)
     * or must remain pending for retry (failure).
     */
    public CompletableFuture<Void> publishAsync(String topic, ExceptionRecordDTO dto) {
        // Key controls partitioning and ordering semantics in Kafka.
        // Using serviceName:securityId keeps related events grouped.
        String key = dto.getServiceName() + ":" + dto.getSecurityId();

        // Sends asynchronously; future completes when broker ACK arrives or fails.
        CompletableFuture<SendResult<String, ExceptionRecordDTO>> future = kafkaTemplate.send(topic, key, dto);

        // Convert producer result future into a Void future expected by caller logic.
        // thenAccept(...) runs only on success and maps value to "no payload" (Void).
        return future.thenAccept(
                        res -> {
                            // Broker metadata helps trace where the message landed.
                            var md = res.getRecordMetadata();
                            // Include topic/partition/offset in logs so replay and audit are easier.
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
                            // Re-throw runtime exception so upstream batch code can keep record pending.
                            // This is key to at-least-once behavior in this project.
                            log.error("Kafka publish failed key={} topic={}", key, topic, ex);
                            throw new RuntimeException(ex);
                        }
                );
    }
}
