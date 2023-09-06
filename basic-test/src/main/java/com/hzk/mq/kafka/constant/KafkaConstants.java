package com.hzk.mq.kafka.constant;

public interface KafkaConstants {

    String BOOTSTRAP_SERVERS = "bootstrap.servers";
    // topic分区
    String MQ_KAFKA_TOPIC_PARTITIONS = "mq.kafka.topic.partitions";
    // kafka消费者工作线程上限
    String MQ_KAFKA_CONSUMER_WORKER_POOL_THREAD_SIZE = "mq.kafka.consumer.worker.pool.size";
    // kafka消费者工作线程池阻塞队列上限
    String MQ_KAFKA_CONSUMER_WORKER_POOL_QUEUE_SIZE = "mq.kafka.consumer.worker.pool.size";
    // 是否开启认证
    String MQ_KAFKA_AUTH_ENABLE = "mq.kafka.auth.enable";

    String MQ_KAFKA_CONSUMER_OFFSET_QUEUE_SIZE = "mq.kafka.consumer.offset.queue.capacity";

    interface DelayConstants {
        // 源topic
        String ORIGIN_TOPIC = "originTopic";
        // 目标topic
        String TARGET_TOPIC = "targetTopic";
        // 开始投递时间，即目标topic收到消息的时间点
        String START_DELIVER_TIME = "startDeliverTime";
    }

    interface RetryConstants {
        // kafka消费最大重试次数
        String MQ_KAFKA_CONSUMER_RETRY_TIMES = "mq.kafka.consumer.retry.times";
        // 重试topic后缀
        String RETRY_TOPIC_SUFFIX = "_retry";
        // 重试次数
        String RETRY_TIMES = "retryTimes";
    }
}
