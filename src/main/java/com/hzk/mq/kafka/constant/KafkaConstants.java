package com.hzk.mq.kafka.constant;

public interface KafkaConstants {

    interface RetryConstants {
        // 最大消费重试次数
        String CONSUMER_MAX_RETRY_NUMBERS = "mq.kafka.consumer.max.retry.numbers";
        // 重试次数
        String RETRY_NUMBERS = "retryNums";

    }

    interface DelayConstants {
        // 目标topic，即源topic
        String TARGET_TOPIC = "targetTopic";
        // 开始投递时间，即目标topic收到消息的时间点
        String START_DELIVER_TIME = "startDeliverTime";
    }

}
