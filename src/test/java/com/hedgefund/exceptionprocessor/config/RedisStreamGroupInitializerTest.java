package com.hedgefund.exceptionprocessor.config;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Tests startup safety behavior in RedisStreamGroupInitializer.
 *
 * Project impact:
 * if this initializer fails to create stream/group correctly, the consumer cannot
 * reliably read from Redis Streams on first boot.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
class RedisStreamGroupInitializerTest {

    @Test
    void createsSeedMessageAndGroupWhenStreamDoesNotExist() throws Exception {
        // Simulate config values read from application.yml.
        AppProperties props = new AppProperties();
        props.getStreams().setRedisStreamName("security.events");
        props.getStreams().setConsumerGroupName("exception-workers");

        // Mock Redis template and stream operations to avoid real Redis dependency.
        StringRedisTemplate redis = Mockito.mock(StringRedisTemplate.class);
        StreamOperations<String, Object, Object> streamOps = Mockito.mock(StreamOperations.class);
        // When code asks for stream ops, return our mock.
        when(redis.opsForStream()).thenReturn(streamOps);
        // Pretend stream key is absent, forcing seed-add path.
        when(redis.hasKey("security.events")).thenReturn(false);
        // Pretend Redis XADD returns new stream id.
        when(streamOps.add(any())).thenReturn(RecordId.of("1-0"));

        // Build initializer under test.
        RedisStreamGroupInitializer initializer = new RedisStreamGroupInitializer(props, redis);
        // Extract startup runner callback.
        ApplicationRunner runner = initializer.createGroupIfMissing();

        // Run startup logic as Spring would do after boot.
        ApplicationArguments args = new DefaultApplicationArguments(new String[]{});
        runner.run(args);

        // Verify seed message is added exactly once for first-time stream creation.
        verify(streamOps, times(1)).add(any());
        // Verify group creation is attempted after stream existence is guaranteed.
        verify(streamOps, times(1)).createGroup("security.events", ReadOffset.latest(), "exception-workers");
    }

    @Test
    void skipsSeedWhenStreamAlreadyExists() throws Exception {
        // Same logical setup, but now stream already exists.
        AppProperties props = new AppProperties();
        props.getStreams().setRedisStreamName("security.events");
        props.getStreams().setConsumerGroupName("exception-workers");

        StringRedisTemplate redis = Mockito.mock(StringRedisTemplate.class);
        StreamOperations<String, Object, Object> streamOps = Mockito.mock(StreamOperations.class);
        when(redis.opsForStream()).thenReturn(streamOps);
        // Existing stream should bypass seed-add path.
        when(redis.hasKey("security.events")).thenReturn(true);

        RedisStreamGroupInitializer initializer = new RedisStreamGroupInitializer(props, redis);
        ApplicationRunner runner = initializer.createGroupIfMissing();
        // Execute startup logic once.
        runner.run(new DefaultApplicationArguments(new String[]{}));

        // Ensure initializer does not pollute stream with extra seed event.
        verify(streamOps, never()).add(any());
        // Group creation still happens (or is attempted) on every boot.
        verify(streamOps, times(1)).createGroup(eq("security.events"), eq(ReadOffset.latest()), eq("exception-workers"));
    }
}
