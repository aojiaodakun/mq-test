package com.hzk.mq.kafka.producer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.hzk.mq.kafka.config.KafkaConfig;
import com.hzk.mq.kafka.constant.KafkaConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * 开源版简单生产者
 * 注：先启动KafkaConsumerMain
 */
public class KafkaRetryProducerMain {

    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = loggerContext.getLogger("root");
        root.setLevel(Level.INFO);
    }


    public static void main(String[] args) throws Exception{
        // 本地
        System.setProperty(KafkaConstants.BOOTSTRAP_SERVERS, "localhost:9092");

        String topic = "retry_test";
        Properties properties = KafkaConfig.getProducerConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // 发送前检测生产者
        Method throwIfProducerClosedMethod = producer.getClass().getDeclaredMethod("throwIfProducerClosed");
        throwIfProducerClosedMethod.setAccessible(true);
        try {
            throwIfProducerClosedMethod.invoke(producer);
        } catch (IllegalStateException e) {
            e.getMessage();
        }

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (int i = 0; i < 1; i++) {
            String value = "value12-02-10";
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
            // 添加头部
            record.headers().add("hzk", "hzk".getBytes());

            // 同步发送
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata recordMetadata = future.get();
            String currDateTime = df.format(new Date());
            System.err.println("topic=" + topic +",dateTime=" + currDateTime + ",key=" + record.key() + ",value=" + record.value()
                    + ",partition=" + recordMetadata.partition() + ",offset=" + recordMetadata.offset());

        }
        producer.close();

    }

}
