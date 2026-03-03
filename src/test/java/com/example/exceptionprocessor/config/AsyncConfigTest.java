package com.example.exceptionprocessor.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

class AsyncConfigTest {

    @Test
    void createsExecutorUsingConfiguredWorkerSettings() {
        AppProperties props = new AppProperties();
        props.getWorker().setCorePoolSize(2);
        props.getWorker().setMaxPoolSize(5);
        props.getWorker().setQueueCapacity(50);
        props.getWorker().setKeepAliveSeconds(45);

        AsyncConfig config = new AsyncConfig(props);
        ThreadPoolTaskExecutor executor = config.exceptionProcessingTaskExecutor();

        assertThat(executor.getCorePoolSize()).isEqualTo(2);
        assertThat(executor.getMaxPoolSize()).isEqualTo(5);
        assertThat(executor.getThreadPoolExecutor().getQueue().remainingCapacity()).isEqualTo(50);
        assertThat(executor.getKeepAliveSeconds()).isEqualTo(45);
        executor.shutdown();
    }
}
