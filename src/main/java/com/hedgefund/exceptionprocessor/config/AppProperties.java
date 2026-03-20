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
    private Streams streams = new Streams();
    private Kafka kafka = new Kafka();
    private Worker worker = new Worker();
    private Retry retry = new Retry();
    private Batch batch = new Batch();
    private Paging paging = new Paging();

    /**
     * Redis stream wiring config.
     *
     *These values are shared by startup initialization, polling, ACK, and reclaim paths.
     */
    @Data
    public static class Streams {
        private String redisStreamName;
        private String consumerGroupName;
        private long maxlen = 1_000_000L;
    }

    /**
     * Kafka output routing config.
     */
    @Data
    public static class Kafka {
        private String topic;
    }

    /**
     * Async executor sizing config for business processing threads.
     */
    @Data
    public static class Worker {
        private int corePoolSize = 4;
        private int maxPoolSize = 4;
        private int queueCapacity = 200;
        private int keepAliveSeconds = 60;
    }

    /**
     * Retry/recovery timing config for pending message reclaim.
     */
    @Data
    public static class Retry {
        private long claimStaleAfterMs = 60_000L;
        private long reclaimerIntervalMs = 30_000L;
    }

    /**
     * Batch size config that controls read pressure and DB query granularity.
     */
    @Data
    public static class Batch {
        private int streamReadCount = 200;
        private int maxInFlightBatches = 4;
        private int securityIdQueryChunkSize = 100;
    }

    /**
     * Pagination config placeholder for future read strategies.
     */
    @Data
    public static class Paging {
        private int pageSize = 1000;
    }
}
