package com.hedgefund.exceptionprocessor.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Defines the dedicated thread pool used for async exception processing.
 *
 *In Spring, a `@Bean` is an object managed by the framework and injected where needed.
 * This pool backs methods annotated with `@Async("exceptionProcessingTaskExecutor")`.
 */
@Configuration
@RequiredArgsConstructor
public class AsyncConfig {
    // Reads worker sizing from configuration so we can tune without code changes.
    private final AppProperties props;

    /**
     * Creates the named executor bean used by {@code @Async} processing methods.
     *
     *Why this matters in this project:
     * Redis consumption should stay responsive while heavier DB+Kafka work runs on
     * separate worker threads.
     */
    @Bean(name = "exceptionProcessingTaskExecutor")
    public ThreadPoolTaskExecutor exceptionProcessingTaskExecutor() {
        // Spring wrapper around Java's thread pool executor.
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // Baseline concurrency level for normal load.
        executor.setCorePoolSize(props.getWorker().getCorePoolSize());
        // Upper concurrency cap to avoid uncontrolled CPU/memory growth.
        executor.setMaxPoolSize(props.getWorker().getMaxPoolSize());
        // Work queue depth before new submissions are rejected.
        executor.setQueueCapacity(props.getWorker().getQueueCapacity());
        // Lifecycle tuning for worker threads.
        executor.setKeepAliveSeconds(props.getWorker().getKeepAliveSeconds());
        // Thread name prefix helps identify processing threads in logs.
        executor.setThreadNamePrefix("proc-");

        executor.setRejectedExecutionHandler(
                (r, e) -> {
                    // We fail fast when saturated so upstream logic can back off/retry.
                    throw new RuntimeException("Task queue full; applying backpressure");
                }
        );

        // Finalizes internal executor construction.
        executor.initialize();
        return executor;
    }
}
