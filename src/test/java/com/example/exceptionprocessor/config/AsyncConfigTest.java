package com.example.exceptionprocessor.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Unit tests for AsyncConfig.
 *
 * These tests verify that infrastructure wiring (thread-pool settings)
 * correctly reflects values from AppProperties.
 */
class AsyncConfigTest {

    @Test
    void createsExecutorUsingConfiguredWorkerSettings() {
        // Build config object exactly like Spring would bind from application.yml.
        AppProperties props = new AppProperties();
        // Configure small non-default numbers so assertion mismatches are obvious.
        props.getWorker().setCorePoolSize(2);
        props.getWorker().setMaxPoolSize(5);
        props.getWorker().setQueueCapacity(50);
        props.getWorker().setKeepAliveSeconds(45);

        // Instantiate config directly (no Spring context needed for this unit test).
        AsyncConfig config = new AsyncConfig(props);
        // Create the actual executor bean instance under test.
        ThreadPoolTaskExecutor executor = config.exceptionProcessingTaskExecutor();

        // Verify each runtime executor value matches AppProperties input.
        assertThat(executor.getCorePoolSize()).isEqualTo(2);
        assertThat(executor.getMaxPoolSize()).isEqualTo(5);
        // Queue remaining capacity equals configured queue size when queue is empty.
        assertThat(executor.getThreadPoolExecutor().getQueue().remainingCapacity()).isEqualTo(50);
        assertThat(executor.getKeepAliveSeconds()).isEqualTo(45);
        // Clean up thread resources so test process exits cleanly.
        executor.shutdown();
    }
}
