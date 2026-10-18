package com.self.kafka.client.tran;

import com.self.kafka.client.demo.KafkaConstant;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Map;

public class TransactionCoordinator {

    private final OrderConsumer consumer;
    private final OrderProducer producer;

    public TransactionCoordinator(OrderConsumer consumer, OrderProducer producer) {
        this.consumer = consumer;
        this.producer = producer;
    }

    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            if (records.isEmpty()) {
                continue;
            }

            producer.begin();
            try {
                // 消费者职责：处理业务
                consumer.process(records);

                // 生产者职责：把位点纳入事务并提交
                Map<TopicPartition, OffsetAndMetadata> offsets = consumer.currentOffsets(records);
                producer.commitOffsets(offsets, consumer.groupMetadata());

            } catch (Exception e) {
                System.err.println("处理失败，回滚: " + e.getMessage());
                producer.abort();
            }
        }
    }

    public static void main(String[] args) {
        String bootstrap = KafkaConstant.brokerList;
        OrderConsumer consumer = new OrderConsumer(bootstrap, "order-processor-group", "input-topic");
        OrderProducer producer = new OrderProducer(bootstrap, "order-processor-tx-1");
        try {
            new TransactionCoordinator(consumer, producer).run();
        } finally {
            consumer.close();
            producer.close();
        }
    }
}
