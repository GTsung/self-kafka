package com.self.kafka.producer.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Data
@Component
public class KfkProperties {

    @Value("${kafka.topic:}")
    private String topic;

}
