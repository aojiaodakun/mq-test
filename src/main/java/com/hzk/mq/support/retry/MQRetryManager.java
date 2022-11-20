package com.hzk.mq.support.retry;

import com.hzk.mq.kafka.delay.KafkaDelayManager;
import com.hzk.mq.support.delay.MetaTime;

public class MQRetryManager {

    /**
     * 获取消费者的重试队列名称
     * @param groupName 消费者组名
     * @return 消费者的重试队列名称
     */
    public static String getRetryTopic(String groupName){
        return MQRetryConstants.RETRY_TOPIC_PREFIX + groupName;
    }

    /**
     * 根据重试次数获取延迟队列名称
     * @param retryNums retryNums
     * @return 延迟队列名称
     */
    public static String getDelayTopic(int retryNums){
        MetaTime metaTime = MetaTime.genInstanceByLevel(retryNums + 2);
        return KafkaDelayManager.getDelayTopicName(metaTime.getName());
    }

}
