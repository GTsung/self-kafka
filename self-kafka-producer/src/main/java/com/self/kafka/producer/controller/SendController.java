package com.self.kafka.producer.controller;

import com.self.kafka.producer.config.KfkProperties;
import com.self.kafka.producer.sender.KafkaSender;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("kfk")
public class SendController {

    @Resource
    private KfkProperties kfkProperties;
    @Resource
    private KafkaSender kafkaSender;

    @GetMapping("send")
    public void send(@RequestParam(name = "value") String value) {
        kafkaSender.send(kfkProperties.getTopic(), value);
    }

}
