package com.hedgefund.exceptionprocessor.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Collections;

/**
 * Ensures Redis stream/group infrastructure exists when the service starts.
 *
 *Without this, first boot could fail to consume because the stream or
 * consumer group has not been created yet.
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class RedisStreamGroupInitializer {
    private final AppProperties props;
    private final StringRedisTemplate redis;

    /**
     * Startup hook that guarantees stream/group exist before consumer loop starts.
     *
     *This removes operational ordering requirements ("create group first manually")
     * and makes local/dev boot smoother.
     */
    @Bean
    ApplicationRunner createGroupIfMissing() {
        return args -> {
            String stream = props.getStreams().getRedisStreamName();
            String group = props.getStreams().getConsumerGroupName();

            try {
                if (Boolean.FALSE.equals(redis.hasKey(stream))) {
                    RecordId id = redis.opsForStream()
                            .add(
                                    StreamRecords.newRecord()
                                            .in(stream)
                                            .ofMap(Collections.singletonMap("init", "1"))
                            );

                    log.info("Created stream {} with seed id={}", stream, id);
                }
            } catch (Exception e) {
                log.debug("Seed add failed (exists?): {}", e.getMessage());
            }

            try {
                redis.opsForStream().createGroup(stream, ReadOffset.latest(), group);
                log.info("Created Redis stream group='{}' on stream='{}'", group, stream);
            } catch (Exception e) {
                String msg = e.getMessage();
                if (msg != null && msg.contains("BUSYGROUP")) {
                    log.info("Group '{}' already exists on stream '{}'", group, stream);
                } else {
                    log.warn("Could not create group: {}", msg);
                }
            }
        };
    }
}
