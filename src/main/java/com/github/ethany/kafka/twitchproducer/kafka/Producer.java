package com.github.ethany.kafka.twitchproducer.kafka;

import com.github.ethany.kafka.twitchproducer.kafka.util.TwitchIrcConnection;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.logging.Logger;

@Component
@RequiredArgsConstructor
class Producer {

    private static final String TOPIC = "twitch";
    private static final Logger LOGGER = Logger.getLogger(Producer.class.getName());
    private final KafkaTemplate<String, String> kafkaTemplate;

    private final RedisTemplate<String, String> redisTemplate;

    @Value("${spring.redis.key}")
    private String key;


    @EventListener(ApplicationStartedEvent.class)
    public void twitch_channels() {

        for (String channel : redisTemplate.opsForList().range(key, 0, -1)) {
            new Thread(TwitchIrcConnection
                    .builder()
                    .channel(channel)
                    .kafkaTemplate(kafkaTemplate)
                    .topic(TOPIC)
                    .logger(LOGGER)
                    .build())
                    .start();
        }
    }
}
