package com.hzk.mq.kafka.producer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.hzk.mq.kafka.config.KafkaConfig;
import com.hzk.mq.kafka.constant.KafkaConstants;
import com.hzk.mq.kafka.util.KafkaAdminUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * 纯测试，不参与demo
 * 1、单个消息最大值
 * 客户端：max.request.size，默认1m
 * 服务端：message.max.bytes
 */
public class KafkaProducerTest {

    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = loggerContext.getLogger("root");
        root.setLevel(Level.INFO);
    }


    public static void main(String[] args) throws Exception{
        // 本地
        System.setProperty(KafkaConstants.BOOTSTRAP_SERVERS, "localhost:9092");


        Properties properties = KafkaConfig.getProducerConfig();
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 8);
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485760);
        String topic = "test1";
//        String topic = "retry_test";
        KafkaAdminUtil.createTopic(topic, 4, (short) 1);


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

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        byte[] bytes = new byte[1024 * 1024 * 1];
        for (int i = 0; i < 2; i++) {
            String value = "value12-19-" + i;
            value = new String(bytes);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);

            // 添加头部
//            record.headers().add("hzk", "hzk".getBytes());


            // 同步发送
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata recordMetadata = future.get();
            String currDateTime = df.format(new Date());
            System.err.println("topic:" + topic +",dateTime:" + currDateTime + ",key:" + record.key() + ",value:" + record.value()
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
