package com.self.kafka.client.tran;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class OrderConsumer implements AutoCloseable {

    private final KafkaConsumer<String, String> consumer;

    public OrderConsumer(String bootstrapServers, String groupId, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public ConsumerRecords<String, String> poll(Duration timeout) {
        return consumer.poll(timeout);
    }

    /**
     * 处理一批消息，失败抛异常
     */
    public void process(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            processOne(record.value());
        }
    }

    private void processOne(String payload) {
        if (payload == null || payload.isBlank()) {
            throw new IllegalArgumentException("invalid payload");
        }
        System.out.println("processed: " + payload);
    }

    /**
     * 计算本批位点：每个分区最后一条 offset + 1
     */
    public Map<TopicPartition, OffsetAndMetadata> currentOffsets(ConsumerRecords<String, String> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition tp : records.partitions()) {
            long lastOffset = records.records(tp)
                    .get(records.records(tp).size() - 1).offset();
            offsets.put(tp, new OffsetAndMetadata(lastOffset + 1));
        }
        return offsets;
    }

    public org.apache.kafka.clients.consumer.ConsumerGroupMetadata groupMetadata() {
        return consumer.groupMetadata();
    }

    @Override
    public void close() {
        consumer.close();
    }
}
