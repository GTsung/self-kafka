package com.self.kafka.client.tran;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

public class OrderProducer implements AutoCloseable {

    private final KafkaProducer<String, String> producer;

    public OrderProducer(String bootstrapServers, String transactionalId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        this.producer = new KafkaProducer<>(props);
        this.producer.initTransactions();
    }

    public void begin() {
        producer.beginTransaction();
    }

    public void send(String topic, String key, String value) {
        producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, key, value));
    }

    public void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets,
                              ConsumerGroupMetadata groupMetadata) {
        producer.sendOffsetsToTransaction(offsets, groupMetadata);
        producer.commitTransaction();
    }

    public void abort() {
        producer.abortTransaction();
    }

    @Override
    public void close() {
        producer.close();
    }
}
