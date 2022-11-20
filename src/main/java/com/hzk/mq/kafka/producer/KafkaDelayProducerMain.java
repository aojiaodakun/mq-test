package com.hzk.mq.kafka.producer;

import com.hzk.mq.kafka.config.KafkaConfig;
import com.hzk.mq.kafka.constant.KafkaConstants;
import com.hzk.mq.kafka.delay.KafkaDelayManager;
import com.hzk.mq.support.delay.DelayControlManager;
import com.hzk.mq.support.delay.MetaTime;
import com.hzk.mq.support.util.ClassCastUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * 发送延迟消息
 */
public class KafkaDelayProducerMain {

    static {
        // 本地
        System.setProperty("bootstrap.servers", "localhost:9092");
    }

    public static void main(String[] args) throws Exception{
        Properties properties = KafkaConfig.getProducerConfig();
        String topic = "delay_test";

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 发送前检测生产者
        Method throwIfProducerClosedMethod = producer.getClass().getDeclaredMethod("throwIfProducerClosed");
        throwIfProducerClosedMethod.setAccessible(true);
        try {
            throwIfProducerClosedMethod.invoke(producer);
        } catch (IllegalStateException e) {
            e.getMessage();
        }

        for (int i = 0; i < 1; i++) {
            String value = "value-delay" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
            // 延迟消息
            int seconds = 7;
            long startDeliverTime = 0;
            MetaTime metaTime = DelayControlManager.selectMaxMetaTime(seconds);
            if (metaTime != MetaTime.delay_1s) {
                String delayTopicName = KafkaDelayManager.getDelayTopicName(metaTime.getName());
                record = new ProducerRecord<>(delayTopicName, value);
                // 目标topic
                record.headers().add(KafkaConstants.DelayConstants.TARGET_TOPIC, topic.getBytes());
                // 开始投递时间
                startDeliverTime = DelayControlManager.getStartDeliverTime(seconds);
                record.headers().add(KafkaConstants.DelayConstants.START_DELIVER_TIME, ClassCastUtil.longToBytes(startDeliverTime));
            }
            // 同步发送
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata recordMetadata = future.get();
            System.err.println("startDeliverTime:" + startDeliverTime + ",currTime:" + System.currentTimeMillis() + ",topic:" + record.topic() + ",value:" + record.value()
                    + ",partition:" + recordMetadata.partition() + ",offset:" + recordMetadata.offset());
        }
        producer.close();
    }





}
