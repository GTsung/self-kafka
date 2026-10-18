package com.self.kafka.ttl.consumer;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class ExpiryAwareConsumer {

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;
    @Value("${kafka.ttlDeadline.topic}")
    private String dltTopic;

    @KafkaListener(topics = "${kafka.business.topic}")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        // 1. 读取消息头中的过期时间
        Header expireHeader = record.headers().lastHeader("X-Message-Expire-At");
        if (expireHeader != null) {
            long expireAt = Long.parseLong(new String(expireHeader.value(), StandardCharsets.UTF_8));
            long now = System.currentTimeMillis();
            // 2. 判断是否已过期
            if (now > expireAt) {
                // 3. 已过期：直接写入死信队列，不执行业务逻辑
                sendToDlt(record, "Message expired at " + expireAt + ", now is " + now);
                ack.acknowledge();
                return;
            }
        }
        // 4. 未过期：执行正常业务逻辑
        processBusiness(record.value(), ack);
    }

    private void sendToDlt(ConsumerRecord<String, String> originalRecord, String reason) {
        try {
            // 将原始消息转发到 DLT，建议保留原始 key/value，并添加失败原因 Header
            ProducerRecord<String, String> dltRecord = new ProducerRecord<>(
                    dltTopic,
                    originalRecord.key(),
                    originalRecord.value()
            );
            // 可选：添加死信原因头，便于后续排查
            dltRecord.headers().add(new RecordHeader("X-DLT-Reason", reason.getBytes(StandardCharsets.UTF_8)));

            // 关键：同步发送并等待结果，确保死信确实写入了 Kafka
            kafkaTemplate.send(dltRecord).get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            // 如果死信发送失败，应该抛出异常，让 Spring Kafka 不要提交 offset，
            // 从而触发重试或进入默认的错误处理流程，避免消息丢失。
            throw new RuntimeException("Failed to send message to DLT", e);
        }
    }

    private void processBusiness(String value, Acknowledgment ack) {
        log.info("process business {}", value);
        ack.acknowledge();
    }

}
