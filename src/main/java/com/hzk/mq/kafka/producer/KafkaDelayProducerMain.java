package com.hzk.mq.kafka.producer;

import com.hzk.mq.kafka.config.KafkaConfig;
import com.hzk.mq.kafka.delay.KafkaDelayConstants;
import com.hzk.mq.kafka.delay.KafkaDelayManager;
import com.hzk.mq.support.delay.DelayControlManager;
import com.hzk.mq.support.delay.MetaTime;
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
            // 延迟消息
            int seconds = 39;
            long startDeliverTime = DelayControlManager.getStartDeliverTime(seconds);
            MetaTime metaTime = DelayControlManager.selectMaxMetaTime(seconds);
            String delayTopicName = KafkaDelayManager.getDelayTopicName(metaTime.getName());

            ProducerRecord<String, String> record = new ProducerRecord<>(delayTopicName, value);
            // 目标topic
            record.headers().add(KafkaDelayConstants.TARGET_TOPIC, topic.getBytes());
            // 开始投递时间
            record.headers().add(KafkaDelayConstants.START_DELIVER_TIME, longToBytes(startDeliverTime));

            // 同步发送
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata recordMetadata = future.get();
            System.err.println("sendCurrTime:" + startDeliverTime + ",currTime:" + System.currentTimeMillis() + ",topic:" + record.topic() + ",value:" + record.value()
                    + ",partition:" + recordMetadata.partition() + ",offset:" + recordMetadata.offset());
        }
        producer.close();
    }

    private static byte[] longToBytes(long longVar) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(longVar).array();
    }



}
