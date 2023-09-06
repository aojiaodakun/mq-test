package com.hzk.listener;


import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.hzk.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;

@Component
public class ApplicationRereshListener implements ApplicationListener<ContextRefreshedEvent>, ApplicationContextAware {


    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = loggerContext.getLogger("root");
        root.setLevel(Level.INFO);
    }


    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        System.out.println("ApplicationRereshListener#setApplicationContext,starting");
        KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.getProducerConfig());
        // 发送前检测生产者
        Method throwIfProducerClosedMethod = null;
        try {
            throwIfProducerClosedMethod = producer.getClass().getDeclaredMethod("throwIfProducerClosed");
            throwIfProducerClosedMethod.setAccessible(true);
            throwIfProducerClosedMethod.invoke(producer);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String topic = System.getProperty("topic", "testConsole");
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (int i = 0; i < 10; i++) {
            String value = "value07-31-" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
            // 同步发送
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata recordMetadata = null;
            try {
                recordMetadata = future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
            String currDateTime = df.format(new Date());
            System.out.println("topic:" + topic +",dateTime:" + currDateTime + ",key:" + record.key() + ",value:" + record.value()
                    + ",partition:" + recordMetadata.partition() + ",offset:" + recordMetadata.offset());
        }
        producer.close();

    }


    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {

    }


}
