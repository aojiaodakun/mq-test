package com.hzk.mq.kafka.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class KafkaConfig {

    public static Properties getProducerConfig(){
        Properties properties = new Properties();
        // 集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("bootstrap.servers"));
        // 序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 请求超时
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        // 发送重试
        properties.put(ProducerConfig.RETRIES_CONFIG, "2");
        // ack模式
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 最大阻塞时间，默认60s
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "3000");


        // 认证
//        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
//        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
//        properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username='admin' password='admin-secret';");

        // SSL模式
        properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "");


        return properties;
    }


    public static Properties getConsumerConfig(){
        Properties properties = new Properties();
        // 集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("bootstrap.servers"));
        // 序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 关闭自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // 消费者offset。earliest，latest
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // 一个分区一次拉取最多n条
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        // 一次拉取最大处理时长，10分钟
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");


        // 认证
//        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
//        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
//        properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username='admin' password='admin-secret';");
        return properties;
    }

}
