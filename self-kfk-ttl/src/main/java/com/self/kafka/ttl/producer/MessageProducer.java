package com.self.kafka.ttl.producer;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Slf4j
@Component
public class MessageProducer {

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;
    @Value("${kafka.business.topic}")
    private String businessTopic;

    public void sendWithTtl(String key, String payload, long ttlMillis) {
        // 计算该条消息独立的过期时间戳
        long expireAt = System.currentTimeMillis() + ttlMillis;

        ProducerRecord<String, String> record = new ProducerRecord<>(businessTopic, key, payload);
        // 将过期时间写入 Header，供消费端读取
        record.headers().add(new RecordHeader("X-Message-Expire-At",
                String.valueOf(expireAt).getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(record);
    }

}
