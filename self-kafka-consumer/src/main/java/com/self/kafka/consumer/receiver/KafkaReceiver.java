package com.self.kafka.consumer.receiver;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaReceiver {

//    @KafkaListener(topics = "${kafka.topic}")
    public void receiveMessage(String message) {
        log.info("receive message: {}", message);
    }

    /**
     * 获取分区偏移量
     */
//    @KafkaListener(topics = "${kafka.topic}")
    public void listenWithMeta(
            String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("消息: {}, 来自分区 {}，偏移量 {}", message, partition, offset);
    }

    /**
     * 整条消息接收
     */
//    @KafkaListener(topics = "${kafka.topic}")
    public void listenRecord(ConsumerRecord<String, String> record) {
        log.info("Key: {}, Value: {}", record.key(), record.value());
    }

    /**
     * 手动提交消息 -> spring.kafka.listener.ack-mode=MANUAL_IMMEDIATE
     * concurrency -> 这个并发数会创建多个消费者进行分区的摊分
     */
    @KafkaListener(topics = "${kafka.topic}", concurrency = "3")
    public void receive(String message, Acknowledgment ack) {
        log.info("receive message: {}", message);
        ack.acknowledge();
    }

}
