package com.self.kafka.client.demo;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;

public class KfkDemoConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstant.brokerList);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstant.consumerGroup);

        // 消费者客户端
//        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-demo");

        // 是否自动提交,默认自动提交消费位移
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // 指定消费位移,默认latest
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Lists.newArrayList(KafkaConstant.topic));

            // 获取此主题所有的分区
//            List<PartitionInfo> topicPartitions = consumer.partitionsFor(KafkaConstant.topic);
//            for (PartitionInfo partitionInfo : topicPartitions) {
//                int partition = partitionInfo.partition(); // 获取分区
//                Node[] nodes = partitionInfo.inSyncReplicas(); // 获取isr
//                // 指定订阅所有分区
//                TopicPartition tp = new TopicPartition(KafkaConstant.topic, partition);
//                consumer.assign(Lists.newArrayList(tp));
//            }

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                }
                // 手动异步提交
//                consumer.commitAsync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
