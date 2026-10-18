package com.self.kafka.consum.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName(value = "t_kafka_failed_message")
public class KafkaFailedMessageDO {

    @TableId(type = IdType.AUTO)
    private Long id;

    @TableField(value = "f_topic")
    private String topic;

    @TableField(value = "f_partition")
    private Integer partition;

    @TableField(value = "f_offset")
    private Long offset;

    @TableField(value = "f_value")
    private String value;

    @TableField(value = "f_error_msg")
    private String errorMsg;

    @TableField(value = "f_created_time")
    private Date createdTime;

}
