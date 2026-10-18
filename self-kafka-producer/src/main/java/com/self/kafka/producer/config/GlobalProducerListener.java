package com.self.kafka.producer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

/**
 * 生产者发送失败统一处理
 * 覆盖 {@link org.springframework.kafka.support.LoggingProducerListener}
 * 这里和发送消息直接whenComplete不会相互覆盖,都会生效
 */
@Slf4j
@Component
public class GlobalProducerListener<K, V> implements ProducerListener<K, V> {

    @Override
    public void onError(ProducerRecord<K, V> producerRecord,
                        RecordMetadata recordMetadata,
                        Exception exception) {
        // 这里就是你实现统一通知逻辑的地方
        log.error("Kafka消息发送失败! Topic: {}, Partition: {}, Key: {}, Value: {}",
                producerRecord.topic(),
                recordMetadata != null ? recordMetadata.partition() : "unknown",
                producerRecord.key(),
                producerRecord.value(),
                exception);

        // 例如：发送告警邮件、写入数据库待补偿表等
    }

}
