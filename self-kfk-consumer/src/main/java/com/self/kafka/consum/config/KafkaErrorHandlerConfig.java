package com.self.kafka.consum.config;

import com.self.kafka.consum.domain.KafkaFailedMessageDO;
import com.self.kafka.consum.mapper.KafkaFailedMessageMapper;
import com.self.kafka.consum.utils.JsonUtil;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;

@Slf4j
@Configuration
public class KafkaErrorHandlerConfig {

    @Resource
    private KafkaFailedMessageMapper kafkaFailedMessageMapper;

    /**
     * DefaultErrorHandler 这个类是在当前分区上进行再次重试,直到重试次数耗尽,才会继续消费当前分区的下一条消息
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        // 初始 3s，每次递增 3s，最多重试 3 次（加上首次共 4 次尝试）
        LinearBackOff backOff = new LinearBackOff(3000L, 3000L, 3L);
        // 重试耗尽后写入数据库
        DefaultErrorHandler handler = new DefaultErrorHandler(
                (record, exception) -> {
                    // 重试耗尽后写入数据库
                    log.error("retry consumer message error, {}", exception.getMessage(), exception);
                    KafkaFailedMessageDO failed = KafkaFailedMessageDO.builder()
                            .topic(record.topic())
                            .partition(record.partition())
                            .offset(record.offset())
                            .value(JsonUtil.toJson(record.value()))
                            .errorMsg(exception.getMessage())
                            .build();
                    kafkaFailedMessageMapper.insert(failed);
                }, backOff);
        // 只有AckMode设置为「MANUAL_IMMEDIATE」才会生效
        // 此值默认为false,改为true后重试完毕不会再次投递
        // 如果为false,虽然失败的消息写入数据库并且不再投递,但依然没有提交位移,当消费实例再次重启后依然重新投递

        // 当下一条消息被成功提交后
        // 消费位置（consumer position）确实因为第二次消费成功而向前推进了，
        // 但已提交偏移量（committed offset） 是否越过了失败消息，取决于容器在后台的提交行为。
        // 在 commitRecovered(false) 且错误处理器没有主动提交的情况下，
        // 那条失败消息的偏移量在 Kafka 看来仍然“未提交”。消费者重启时，会从上次已提交的偏移量开始拉取，失败消息自然会被再次投递

        // 在 commitRecovered(false) 且使用了手动确认 + 错误处理器的组合下，
        // Spring Kafka 并不保证“后续消息的成功提交”会覆盖“失败消息的未提交状态”
        // kafka会再次seek到这条失败消息的位移量,从此处再次消费,这就会导致下一条成功消费的消息也再次消费
        handler.setCommitRecovered(true);
        return handler;
    }

}
