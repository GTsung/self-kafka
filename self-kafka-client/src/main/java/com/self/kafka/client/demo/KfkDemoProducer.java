package com.self.kafka.client.demo;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KfkDemoProducer {

    /**
     * 生产者是线程安全的,一般池化
     */
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstant.brokerList);

        // client.id 生产者对应的clientId,不设置会自动填充
//        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kfk-demo-producer");

        // 设置重试次数,默认0
//        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // 设置ack, 0-发完即可,不关心是否发送到分区; 1-只发送到主分区即可; all-全部分区成功
//        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // 开启幂等,默认就是开启,幂等指的是只要broker存在这条消息就不会重复写入
//        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> producerRecord
                        = new ProducerRecord<>(KafkaConstant.topic, "hello world " + i);
                // 异步发送,这里接收异步结果,可用于判断是否发送成功
//                Future<RecordMetadata> future = producer.send(producerRecord);
//                RecordMetadata recordMetadata = future.get();
//                System.out.println(recordMetadata);

                producer.send(producerRecord, new KfkCallback());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 发送失败回调函数,当重试次数发送完仍然失败回调
     */
    public static class KfkCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                // 这里可以做发送失败操作
                exception.printStackTrace();
            } else {
                System.out.println(metadata);
            }
        }
    }

    //  key -默认空,可以用来进行分区的计算,相同的key发往相同的分区

    // public class ProducerRecord<K, V> {
    //
    //    private final String topic;
    //    private final Integer partition;
    //    private final Headers headers;
    //    private final K key;
    //    private final V value;
    //    private final Long timestamp;
    // }


    // 发送消息-> 拦截器 -> 序列化 -> 分区 -> 累加消息+时间间隔才会发送;buffer.memory ; max.block.ms
    // 分区-> 可自定义设置,
    // 如果消息中有key则会计算key的hash值发送到所有分区的任一个分区,
    // 如果消息没有key则会轮询发送到可用分区的任一个
}
