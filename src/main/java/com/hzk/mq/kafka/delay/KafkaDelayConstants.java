package com.hzk.mq.kafka.delay;

public interface KafkaDelayConstants {

    // 目标topic，即源topic
    String TARGET_TOPIC = "targetTopic";
    // 开始投递时间，即目标topic收到消息的时间点
    String START_DELIVER_TIME = "startDeliverTime";


}
