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
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(props.getWorker().getCorePoolSize());
        executor.setMaxPoolSize(props.getWorker().getMaxPoolSize());
        executor.setQueueCapacity(props.getWorker().getQueueCapacity());
        executor.setKeepAliveSeconds(props.getWorker().getKeepAliveSeconds());
        executor.setThreadNamePrefix("proc-");

        executor.setRejectedExecutionHandler(
                (r, e) -> {
                    throw new RuntimeException("Task queue full; applying backpressure");
                }
        );

        executor.initialize();
        return executor;
    }
}
