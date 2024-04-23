package com.hzk;

import com.hzk.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Future;

public class KafkaApplicationMain {

    /**
     * bootstrap.servers=localhost:9092 topic=testConsole group=testConsole
     * bootstrap.servers=172.17.7.78:9092 topic=testConsole group=testConsole
     * @param args
     */
    public static void main(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String[] tempArr = args[i].split("=");
            System.setProperty(tempArr[0], tempArr[1]);
        }
        producerTest();
    }

    private static void producerTest() {
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

}
