package com.example.exceptionprocessor.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.exceptionprocessor.config.AppProperties;
import com.example.exceptionprocessor.dto.ExceptionRecordDTO;
import com.example.exceptionprocessor.persistence.ExceptionRecord;
import com.example.exceptionprocessor.persistence.Severity;
import com.example.exceptionprocessor.repo.ExceptionRecordRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
@ActiveProfiles("integrationtest")
@TestPropertySource(properties = {
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "app.streams.redis-stream-name=security.events.it",
        "app.streams.consumer-group-name=exception-workers.it",
        "app.kafka.topic=exception-records.it",
        "app.batch.stream-read-count=25",
        "app.batch.max-in-flight-batches=2",
        "app.retry.claim-stale-after-ms=2500",
        "app.retry.reclaimer-interval-ms=1000"
})
class ExceptionFlowIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Container
    static final GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine")).withExposedPorts(6379);

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired
    private ExceptionRecordRepository repo;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private AppProperties appProperties;

    @BeforeEach
    void cleanState() {
        repo.deleteAll();
    }

    @Test
    void redisTriggerPublishesKafkaAndMarksRowsProcessed() {
        ExceptionRecord first = repo.save(record("SEC_E2E", "first error"));
        ExceptionRecord second = repo.save(record("SEC_E2E", "second error"));

        Map<String, Object> consumerProps =
                org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps(
                        "it-group-" + UUID.randomUUID(),
                        "true",
                        kafka.getBootstrapServers()
                );
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.exceptionprocessor.dto");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, ExceptionRecordDTO.class.getName());
        consumerProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        DefaultKafkaConsumerFactory<String, ExceptionRecordDTO> factory =
                new DefaultKafkaConsumerFactory<>(consumerProps);

        try (Consumer<String, ExceptionRecordDTO> consumer = factory.createConsumer()) {
            consumer.subscribe(List.of(appProperties.getKafka().getTopic()));
            consumer.poll(Duration.ofMillis(500));

            redisTemplate.opsForStream().add(
                    appProperties.getStreams().getRedisStreamName(),
                    Map.of("securityId", "SEC_E2E", "trigger", "it")
            );

            List<ExceptionRecordDTO> events = waitForMessages(consumer, 2, Duration.ofSeconds(30));
            assertThat(events).hasSize(2);
            assertThat(events).allMatch(dto -> "SEC_E2E".equals(dto.getSecurityId()));

            waitUntilProcessed(List.of(first.getId(), second.getId()), Duration.ofSeconds(30));
        }

        ExceptionRecord firstUpdated = repo.findById(first.getId()).orElseThrow();
        ExceptionRecord secondUpdated = repo.findById(second.getId()).orElseThrow();
        assertThat(firstUpdated.getProcessedAt()).isNotNull();
        assertThat(secondUpdated.getProcessedAt()).isNotNull();
    }

    private List<ExceptionRecordDTO> waitForMessages(
            Consumer<String, ExceptionRecordDTO> consumer,
            int expectedCount,
            Duration timeout
    ) {
        long deadline = System.nanoTime() + timeout.toNanos();
        List<ExceptionRecordDTO> records = new ArrayList<>();
        while (System.nanoTime() < deadline && records.size() < expectedCount) {
            consumer.poll(Duration.ofMillis(500))
                    .forEach(rec -> records.add(rec.value()));
        }
        return records;
    }

    private void waitUntilProcessed(List<Long> ids, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            boolean allProcessed = ids.stream()
                    .map(id -> repo.findById(id).orElseThrow())
                    .allMatch(r -> r.getProcessedAt() != null);
            if (allProcessed) {
                return;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for processed records", e);
            }
        }
        throw new AssertionError("Timed out waiting for records to be marked as processed");
    }

    private static ExceptionRecord record(String securityId, String message) {
        return ExceptionRecord.builder()
                .serviceName("svc-it")
                .severity(Severity.HIGH)
                .message(message)
                .occurredAt(Instant.now())
                .securityId(securityId)
                .build();
    }
}
