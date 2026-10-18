package com.self.kafka.consumer.nonblock;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Hello world!
 */
@MapperScan(basePackages = {"com.self.kafka.consumer.nonblock.mapper"})
@SpringBootApplication
public class NonblockConsumerApp {

    public static void main(String[] args) {
        SpringApplication.run(NonblockConsumerApp.class, args);
    }
}
