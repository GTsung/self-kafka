package com.self.kafka.consum;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@MapperScan(basePackages = {"com.self.kafka.consum.mapper"})
@SpringBootApplication
public class ConApp {

    public static void main(String[] args) {
        SpringApplication.run(ConApp.class, args);
    }

}
