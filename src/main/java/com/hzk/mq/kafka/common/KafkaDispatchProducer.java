package com.hzk.mq.kafka.common;

import com.hzk.mq.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

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

}
