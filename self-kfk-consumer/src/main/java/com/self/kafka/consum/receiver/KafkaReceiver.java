package com.self.kafka.consum.receiver;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaReceiver {

    /**
     * 手动提交消息,这里的 concurrency 为几个消费者,这个topic有3个分区
     * 通过 {@link com.self.kafka.consum.config.KafkaErrorHandlerConfig} 进行重试操作
     * 这个重试是发生在同一个分区进行的,只有这条消息处理成功后才会消费下一条消息,保证了同一分区的顺序
     */
    @KafkaListener(topics = "${kafka.topic}", concurrency = "3")
    public void receiveMessage(String message, Acknowledgment ack) {
        log.info("receive message: {}", message);
        // todo 防止kafka reBalance 导致同一时间两个消费者同时消费同一条消息,这里可以使用分布式锁
        // todo 在进入到分布式锁内后执行幂等
        try {
            int i = Integer.parseInt(message);
            log.info("receive message success: {}", i);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("receive message error, {}", e.getMessage(), e);
            // 抛出来才会重新消费,否则会默认正常,等到下一条消息消费成功后会提交最新位移导致异常消息丢失
            throw e;
        }
    }


}
