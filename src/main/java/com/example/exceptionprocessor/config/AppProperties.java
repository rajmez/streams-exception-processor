package com.example.exceptionprocessor.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "app")
public class AppProperties {
    private Streams streams = new Streams();
    private Kafka kafka = new Kafka();
    private Worker worker = new Worker();
    private Retry retry = new Retry();
    private Paging paging = new Paging();

    @Data
    public static class Streams {
        private String streamName;
        private String groupName;
        private long maxlen = 1_000_000L;
    }

    @Data
    public static class Kafka {
        private String topic;
    }

    @Data
    public static class Worker {
        private int corePoolSize = 4;
        private int maxPoolSize = 4;
        private int queueCapacity = 200;
        private int keepAliveSeconds = 60;
    }

    @Data
    public static class Retry {
        private long claimStaleAfterMs = 60_000L;
        private long reclaimerIntervalMs = 30_000L;
    }

    @Data
    public static class Paging {
        private int pageSize = 1000;
    }
}
