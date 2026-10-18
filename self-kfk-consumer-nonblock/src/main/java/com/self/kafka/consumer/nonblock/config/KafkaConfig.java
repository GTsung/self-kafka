package com.self.kafka.consumer.nonblock.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * 构建两个工厂,可以使得消费者自己选择采用哪种模式
 * 注解： @KafkaListener(topics = "topic-manual", containerFactory = "manualAckFactory")
 */
@Configuration
public class KafkaConfig {

    // 工厂 A：手动提交
    @Bean("manualAckFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> manualAckFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    // 工厂 B：RECORD 模式
    @Bean("recordAckFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> recordAckFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }

}
