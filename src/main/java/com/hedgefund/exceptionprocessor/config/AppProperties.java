package com.hedgefund.exceptionprocessor.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Centralized configuration model for all custom `app.*` settings in `application.yml`.
 *
 *`@ConfigurationProperties(prefix = "app")` means Spring reads keys like
 * `app.streams.redis-stream-name` and copies values into this object.
 *
 *This avoids hardcoding infra/runtime values in Java code and makes behavior
 * tunable per environment.
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "app")
public class AppProperties {
    // Settings for Redis Streams consumption.
    private Streams streams = new Streams();
    // Settings for Kafka output topic.
    private Kafka kafka = new Kafka();
    // Settings for local async worker thread pool.
    private Worker worker = new Worker();
    // Settings for reclaiming unacked stale messages.
    private Retry retry = new Retry();
    // Settings for read and processing batching.
    private Batch batch = new Batch();
    // Settings for DB query page/chunk sizes.
    private Paging paging = new Paging();

    /**
     * Redis stream wiring config.
     *
     *These values are shared by startup initialization, polling, ACK, and reclaim paths.
     */
    @Data
    public static class Streams {
        // Redis stream name this service reads from (for example: security.events).
        private String redisStreamName;
        // Redis consumer group name for horizontal scaling across instances.
        private String consumerGroupName;
        // Intended max stream length policy used by producers/ops scripts.
        private long maxlen = 1_000_000L;
    }

    /**
     * Kafka output routing config.
     */
    @Data
    public static class Kafka {
        // Kafka topic where transformed exception records are published.
        private String topic;
    }

    /**
     * Async executor sizing config for business processing threads.
     */
    @Data
    public static class Worker {
        // Minimum worker threads kept alive for async processing.
        private int corePoolSize = 4;
        // Maximum worker threads; equal to core here for stable throughput.
        private int maxPoolSize = 4;
        // Queue size before we apply backpressure via rejection.
        private int queueCapacity = 200;
        // Idle timeout before excess threads are removed.
        private int keepAliveSeconds = 60;
    }

    /**
     * Retry/recovery timing config for pending message reclaim.
     */
    @Data
    public static class Retry {
        // Pending message idle threshold before we treat it as stale and reclaim it.
        private long claimStaleAfterMs = 60_000L;
        // How often the scheduled reclaimer checks Redis pending entries.
        private long reclaimerIntervalMs = 30_000L;
    }

    /**
     * Batch size config that controls read pressure and DB query granularity.
     */
    @Data
    public static class Batch {
        // How many stream entries to fetch per Redis read call.
        private int streamReadCount = 200;
        // Max batches processed concurrently in this JVM.
        private int maxInFlightBatches = 4;
        // Query chunk size for `securityId IN (...)` DB fetches.
        private int securityIdQueryChunkSize = 100;
    }

    /**
     * Pagination config placeholder for future read strategies.
     */
    @Data
    public static class Paging {
        // Reserved for future pagination tuning when large DB scans are needed.
        private int pageSize = 1000;
    }
}
