package com.self.kafka.consumer.nonblock.receiver;

import com.self.kafka.consumer.nonblock.domain.KafkaFailedMessageDO;
import com.self.kafka.consumer.nonblock.mapper.KafkaFailedMessageMapper;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;

@Slf4j
@Component
public class KafkaReceiver {

    @Resource
    private KafkaFailedMessageMapper kafkaFailedMessageMapper;

    /**
     * 如果消息消费失败转到专门的重试topic里进行重试,不影响此分区的下一条消息-》非阻塞
     * 非阻塞会影响同一分区的消息消费顺序
     * 这里不再需要使用 Acknowledgment 手动提交, 配置  ack-mode 为 RECORD
     */
    @RetryableTopic(
            attempts = "4",                    // 总共尝试4次（首次 + 3次重试）
            backoff = @Backoff(delay = 3000, multiplier = 2.0),  // 3s, 6s, 12s
            autoCreateTopics = "true",
            numPartitions = "3", // 这里和原topic分区数一致,默认使用和原topic一样的分区数进行转发,分区少于原topic可能会转发失败
            replicationFactor = "3" // 副本和原来也一样
    )
    @KafkaListener(
            topics = "${kafka.nonblock.topic}",
            concurrency = "3",
            containerFactory = "recordAckFactory") // 指定模式
    public void receiveMessage(String message) {
        log.info("receive message: {}", message);
        // todo 防止kafka reBalance 导致同一时间两个消费者同时消费同一条消息,这里可以使用分布式锁
        // todo 在进入到分布式锁内后执行幂等
        try {
            int i = Integer.parseInt(message);
            log.info("receive message success: {}", i);
        } catch (Exception e) {
            log.error("receive message error, {}", e.getMessage(), e);
            // 抛出来才会重新消费,否则会默认正常,等到下一条消息消费成功后会提交最新位移导致异常消息丢失
            throw e;
        }
    }

    // 最终重试全部失败后，进入到死信队列
    @DltHandler
    public void handleDlt(@Payload String failedMessage,
                          @Header(KafkaHeaders.ORIGINAL_TOPIC) String originalTopic,
                          @Header(KafkaHeaders.ORIGINAL_PARTITION) int originalPartition,
                          @Header(KafkaHeaders.ORIGINAL_OFFSET) byte[] originalOffsetBytes,
                          @Header(KafkaHeaders.EXCEPTION_MESSAGE) String errorMessage) {
        // 偏移量是 byte[] 类型，需要转换
        long originalOffset = ByteBuffer.wrap(originalOffsetBytes).getLong();
        log.error("DLT消息: topic={}, partition={}, offset={}, error={}, value={}",
                originalTopic, originalPartition, originalOffset, errorMessage, failedMessage);
        // 执行你之前的数据库写入逻辑
        KafkaFailedMessageDO kafkaFailedMessageDO = KafkaFailedMessageDO.builder()
                .topic(originalTopic)
                .partition(originalPartition)
                .offset(originalOffset)
                .errorMsg(errorMessage)
                .value(failedMessage)
                .build();
        kafkaFailedMessageMapper.insert(kafkaFailedMessageDO);
    }


}
