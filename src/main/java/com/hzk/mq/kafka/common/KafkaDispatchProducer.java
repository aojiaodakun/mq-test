package com.hzk.mq.kafka.common;

import com.hzk.mq.kafka.config.KafkaConfig;
import com.hzk.mq.kafka.constant.KafkaConstants;
import com.hzk.mq.kafka.delay.KafkaDelayManager;
import com.hzk.mq.support.delay.DelayControlManager;
import com.hzk.mq.support.delay.MetaTime;
import com.hzk.mq.support.util.ClassCastUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaDispatchProducer {

    private static KafkaProducer<String, String> DISPATCH_PRODUCER;

    static {
        Properties properties = KafkaConfig.getProducerConfig();
        DISPATCH_PRODUCER = new KafkaProducer<>(properties);

    }

    public static Future<RecordMetadata> send(ProducerRecord<String, String> record){
        return DISPATCH_PRODUCER.send(record);
    }

    /**
     * 发送延迟消息
     * @param record record
     * @param delaySeconds delaySeconds
     * @return Future
     */
    public static Future<RecordMetadata> send(ProducerRecord<String, String> record, int delaySeconds){
        if (delaySeconds <= 0){
            return send(record);
        }
        // 仅支持2h内的任意秒
        validateTime(delaySeconds);
        MetaTime metaTime = DelayControlManager.selectMaxMetaTime(delaySeconds);
        if (metaTime != MetaTime.delay_1s) {
            String targetTopic = record.topic();
            String delayTopicName = KafkaDelayManager.getDelayTopicName(metaTime.getName());
            record = new ProducerRecord<>(delayTopicName, record.value());
            // 源topic
            record.headers().add(KafkaConstants.DelayConstants.ORIGIN_TOPIC, targetTopic.getBytes());
            // 目标topic
            record.headers().add(KafkaConstants.DelayConstants.TARGET_TOPIC, targetTopic.getBytes());
            // 开始投递时间
            long startDeliverTime = DelayControlManager.getStartDeliverTime(delaySeconds);
            record.headers().add(KafkaConstants.DelayConstants.START_DELIVER_TIME, ClassCastUtil.longToBytes(startDeliverTime));
        }
        return DISPATCH_PRODUCER.send(record);
    }

    private static void validateTime(int seconds) {
        int maxDelayTime = 7200;
        if (seconds > maxDelayTime) {
            throw new IllegalArgumentException("max delay time is "+maxDelayTime+" seconds");
        }
    }

}
