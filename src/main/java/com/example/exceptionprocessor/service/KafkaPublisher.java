package com.example.exceptionprocessor.service;

import com.example.exceptionprocessor.dto.ExceptionRecordDTO;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaPublisher {
    private final KafkaTemplate<String, ExceptionRecordDTO> kafkaTemplate;

    public CompletableFuture<Void> publishAsync(String topic, ExceptionRecordDTO dto) {
        String key = dto.getServiceName() + ":" + dto.getSecurityId();

        CompletableFuture<SendResult<String, ExceptionRecordDTO>> future = kafkaTemplate.send(topic, key, dto);

        // Use thenAccept so the returned future is strongly typed as CompletableFuture<Void>.
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
