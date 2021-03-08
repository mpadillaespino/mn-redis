package com.mfpe;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.LocalTime;

@Controller("/time")
public class RateLimitedTimeEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(RateLimitedTimeEndpoint.class);
    private static final Integer QUOTA_PER_MINUTE = 10;
    private StatefulRedisConnection<String, String> redis;

    public RateLimitedTimeEndpoint(StatefulRedisConnection<String, String> redis) {
        this.redis = redis;
    }

    @Get("/")
    public String time(){
        return getTime("EXAMPLE::TIME",LocalTime.now());
    }

    @ExecuteOn(TaskExecutors.IO)
    @Get("/utc")
    public String utc(){
        return getTime("EXAMPLE::UTC",LocalTime.now(Clock.systemUTC()));
    }

    private String getTime(String key, LocalTime now){
        String value = redis.sync().get(key);
        int currentQuota = null == value ? 0 : Integer.parseInt(value);
        if(currentQuota >= QUOTA_PER_MINUTE){
            String error = String.format("Rate limit reached %s %s/%s", key, currentQuota, QUOTA_PER_MINUTE);
            LOG.info(error);
            return error;
        }
        LOG.info("Current quota {} in {}/{}", key, currentQuota, QUOTA_PER_MINUTE);
        increaseCurrentQuota(key);
        return now.toString();
    }

    private void increaseCurrentQuota(String key) {
        RedisCommands<String, String> commands = redis.sync();
        // multi
        commands.multi();
        // increase
        commands.incrby(key, 1);
        // expire
        long remainingSeconds = 60 - LocalTime.now().getSecond();
        commands.expire(key, remainingSeconds);
        // execute
        commands.exec();
    }

}
