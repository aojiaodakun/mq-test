package com.hzk.mq.kafka.producer;

import com.hzk.mq.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaByteProducerMain {


    public static void main(String[] args) throws Exception{
        Properties properties = KafkaConfig.getProducerConfig();
        String topic = "test1";

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);
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

        for (int i = 0; i < 10; i++) {
            String value = "value11-09-" + i;
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, value.getBytes());

            // 添加头部
            record.headers().add("hzk", "hzk".getBytes());



            // 同步发送
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata recordMetadata = future.get();
            System.out.println("key:" + record.key() + ",value:" + record.value()
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
