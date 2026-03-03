package com.example.exceptionprocessor.config;

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

@SuppressWarnings({"unchecked", "rawtypes"})
class RedisStreamGroupInitializerTest {

    @Test
    void createsSeedMessageAndGroupWhenStreamDoesNotExist() throws Exception {
        AppProperties props = new AppProperties();
        props.getStreams().setRedisStreamName("security.events");
        props.getStreams().setConsumerGroupName("exception-workers");

        StringRedisTemplate redis = Mockito.mock(StringRedisTemplate.class);
        StreamOperations<String, Object, Object> streamOps = Mockito.mock(StreamOperations.class);
        when(redis.opsForStream()).thenReturn(streamOps);
        when(redis.hasKey("security.events")).thenReturn(false);
        when(streamOps.add(any())).thenReturn(RecordId.of("1-0"));

        RedisStreamGroupInitializer initializer = new RedisStreamGroupInitializer(props, redis);
        ApplicationRunner runner = initializer.createGroupIfMissing();

        ApplicationArguments args = new DefaultApplicationArguments(new String[]{});
        runner.run(args);

        verify(streamOps, times(1)).add(any());
        verify(streamOps, times(1)).createGroup("security.events", ReadOffset.latest(), "exception-workers");
    }

    @Test
    void skipsSeedWhenStreamAlreadyExists() throws Exception {
        AppProperties props = new AppProperties();
        props.getStreams().setRedisStreamName("security.events");
        props.getStreams().setConsumerGroupName("exception-workers");

        StringRedisTemplate redis = Mockito.mock(StringRedisTemplate.class);
        StreamOperations<String, Object, Object> streamOps = Mockito.mock(StreamOperations.class);
        when(redis.opsForStream()).thenReturn(streamOps);
        when(redis.hasKey("security.events")).thenReturn(true);

        RedisStreamGroupInitializer initializer = new RedisStreamGroupInitializer(props, redis);
        ApplicationRunner runner = initializer.createGroupIfMissing();
        runner.run(new DefaultApplicationArguments(new String[]{}));

        verify(streamOps, never()).add(any());
        verify(streamOps, times(1)).createGroup(eq("security.events"), eq(ReadOffset.latest()), eq("exception-workers"));
    }
}
