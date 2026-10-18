package com.self.kafka.producer.sender;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaSender {

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    public void send(String topic, String key, String value) {
        kafkaTemplate.send(topic, key, value).whenComplete(
                (recordMetadata, throwable) -> {
                    // 尽管已经有 GlobalProducerListener 全局配置,但是不冲突,都会生效
                    if (throwable != null) {
                        log.error("send error", throwable);
                    }
                });
    }

    public void send(String topic, String value) {
        log.info("topic: {}, send message: {}", topic, value);
        kafkaTemplate.send(topic, value);
    }

}
