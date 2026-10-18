CREATE TABLE `t_kafka_failed_message` (
  `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `f_topic` VARCHAR(255) DEFAULT '' NOT NULL COMMENT 'Kafka主题',
  `f_partition` INT NOT NULL COMMENT '分区号',
  `f_offset` BIGINT NOT NULL COMMENT '偏移量',
  `f_value` json COMMENT '消息内容',
  `f_error_msg` TEXT COMMENT '错误信息',
  `f_created_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Kafka消费失败消息记录表';

