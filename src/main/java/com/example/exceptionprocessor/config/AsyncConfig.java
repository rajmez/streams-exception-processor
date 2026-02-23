package com.example.exceptionprocessor.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@RequiredArgsConstructor
public class AsyncConfig {
    private final AppProperties props;

    @Bean(name = "processingExecutor")
    public ThreadPoolTaskExecutor processingExecutor() {
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
