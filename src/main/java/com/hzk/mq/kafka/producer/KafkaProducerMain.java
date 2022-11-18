package com.hzk.mq.kafka.producer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.hzk.mq.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerMain {

    static {
        // 本地
        System.setProperty("bootstrap.servers", "localhost:9092");
//        System.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        // 虚机
//        System.setProperty("bootstrap.servers", "172.20.158.201:9092,172.20.158.201:9093,172.20.158.201:9094");


        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = loggerContext.getLogger("root");
        root.setLevel(Level.INFO);
    }


    public static void main(String[] args) throws Exception{
        Properties properties = KafkaConfig.getProducerConfig();
        String topic = "test1";

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 发送前检测生产者
        Method throwIfProducerClosedMethod = producer.getClass().getDeclaredMethod("throwIfProducerClosed");
        throwIfProducerClosedMethod.setAccessible(true);
        try {
            throwIfProducerClosedMethod.invoke(producer);
        } catch (IllegalStateException e) {
            e.getMessage();
        }
        // 获取kafka配置
        Field kafkaConfigField = producer.getClass().getDeclaredField("producerConfig");
        kafkaConfigField.setAccessible(true);
        ProducerConfig producerConfig = (ProducerConfig)kafkaConfigField.get(producer);
        System.out.println(producerConfig);

        for (int i = 0; i < 1; i++) {
            String value = "value11-11-" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);

            // 添加头部
            record.headers().add("hzk", "hzk".getBytes());


            // 同步发送
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata recordMetadata = future.get();
            System.err.println("key:" + record.key() + ",value:" + record.value()
                    + ",partition:" + recordMetadata.partition() + ",offset:" + recordMetadata.offset());


            // 异步发送
//            producer.send(record, ((metadata, exception) -> {
//                if (exception == null) {
//                    System.out.println("key:" + record.key() + ",value:" + record.value()
//                            + ",partition:" + metadata.partition() + ",offset:" + metadata.offset());
//                } else {
//                    exception.printStackTrace();
//                }
//            }));


        }
        producer.close();


    }

}
