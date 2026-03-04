package com.example.exceptionprocessor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point of the Spring Boot application.
 *
 *Spring Boot is a framework that wires application components automatically.
 * These annotations tell Spring to:
 * 1) discover our classes and create objects ("beans"),
 * 2) allow background async methods,
 * 3) run scheduled jobs (used by the Redis stale-message reclaimer).
 */
@SpringBootApplication
@EnableAsync
@EnableScheduling
public class ExceptionServiceApplication {
    /**
     * JVM entry method.
     *
     *From here Spring starts all configured beans, including:
     * - Redis stream initializer,
     * - stream consumer poller,
     * - async worker pool,
     * - scheduled stale-message reclaimer.
     */
    public static void main(String[] args) {
        // Bootstraps the Spring container and starts the embedded web runtime.
        SpringApplication.run(ExceptionServiceApplication.class, args);
    }
}
