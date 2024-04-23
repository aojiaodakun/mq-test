package com.hzk.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaConfig {

    private static final String SECURITY_PROTOCOL = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
    private static final String SASL_JAAS_CONFIG = SaslConfigs.SASL_JAAS_CONFIG;
    private static final String SASL_MECHANISM = SaslConfigs.SASL_MECHANISM;


    public static Properties getProducerConfig(){
        Properties properties = new Properties();
        // 集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("bootstrap.servers"));
        // 序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 请求超时
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        // 发送重试
        properties.put(ProducerConfig.RETRIES_CONFIG, "2");
        // ack模式
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 最大阻塞时间，默认60s
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
        // 消息体上限，10M
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485760);
        // 消息幂等性，默认true
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, System.getProperty("enable.idempotence", "true"));


        // SSL
        String securityProtocol = System.getProperty("securityProtocol");
        if (securityProtocol != null && !securityProtocol.equals("")) {
            String userName = System.getProperty("userName");
            String password = System.getProperty("password");
            String saslMechanism = System.getProperty("saslMechanism");

            String config = getKafkaAuthConfig(userName, password);
            String mechanism = saslMechanism == null ? "PLAIN" : saslMechanism;

            properties.put(SASL_MECHANISM, mechanism);
            properties.put(SECURITY_PROTOCOL, securityProtocol);
            properties.put(SASL_JAAS_CONFIG, config);
        }
        System.out.println(properties);
        return properties;
    }



    private static String getKafkaAuthConfig(String userName, String pw) {
        if (userName != null && !userName.equals("")) {
            throw new RuntimeException("Config item 'userName' of kafka appender can't be empty when securityProtocol is 'SASL_PLAINTEXT'.");
        }
        if (pw != null && !pw.equals("")) {
            throw new RuntimeException("Config item 'password' of kafka appender can't be empty when securityProtocol is 'SASL_PLAINTEXT'.");
        }
        System.setProperty("logKafkaUser", userName); //系统中其他kafka，默认用此userName
        System.setProperty("logKafkaPwd", pw); //系统中其他kafka，默认用此password

        return "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + userName + "\" password=\"" + pw + "\";";
    }

}
