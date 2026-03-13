package com.hzk.mq.kafka.config;

import com.hzk.mq.kafka.constant.KafkaConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
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
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty(KafkaConstants.BOOTSTRAP_SERVERS, "localhost:9092"));
        // 序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 发送重试
        properties.put(ProducerConfig.RETRIES_CONFIG, "2");
        // ack模式
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 最大阻塞时间180s，多地区集群获取metadata失败问题
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 180000);
        // 请求超时
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 180000);
        properties.put("socket.connection.setup.timeout.ms", 10000);
        properties.put("socket.connection.setup.timeout.max.ms", 30000);
        // 认证
        putAuthConfig(properties);
        return properties;
    }


    public static Properties getConsumerConfig(){
        Properties properties = new Properties();
        // 集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty(KafkaConstants.BOOTSTRAP_SERVERS, "localhost:9092"));
        // 序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 关闭自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 消费者offset。earliest，latest
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // 一个分区一次拉取最多n条
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        // 一次拉取最大处理时长，10分钟
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");
        // 认证
        putAuthConfig(properties);
        return properties;
    }

    public static Properties getAdminConfig(){
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty(KafkaConstants.BOOTSTRAP_SERVERS, "localhost:9092"));
        properties.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000");
        // 认证
        putAuthConfig(properties);
        return properties;
    }


    /**
     * 1. 无认证模式（PLAINTEXT）
     * 默认情况下 Kafka 是明文通信，不启用任何认证。
     * 只适用于测试或内网环境，生产环境不推荐。
     *
     * 2. SSL（基于证书的认证）
     * Kafka Broker 与客户端之间通过 TLS/SSL 双向认证。
     * 客户端需要持有证书，Broker 验证证书身份。
     *
     * 3. SASL（简单认证和安全层）机制
     * Kafka 使用 SASL 框架，可以选择不同的机制来认证，支持的有：
     * 🔹 SASL/PLAIN
     * 明文用户名密码认证（一般配合 SSL 使用，否则密码明文传输不安全）。
     * 🔹 SASL/SCRAM
     * 更安全的用户名密码认证（基于 Salted Challenge Response Authentication Mechanism）。
     * Kafka 支持两种 SCRAM 算法：
     * SCRAM-SHA-256
     * SCRAM-SHA-512
     * 🔹 SASL/GSSAPI（Kerberos）
     * 基于 Kerberos 的认证，常用于企业环境。
     * 🔹 SASL/OAUTHBEARER
     * 基于 OAuth 2.0 的 Bearer Token 认证。
     * 适用于需要统一接入 OAuth 身份认证系统的场景。
     *
     *
     * 总结
     * 1、SSL
     * 2、SASL/PLAIN
     * 3、SASL/SCRAM-SHA-256
     * 4、SASL/SCRAM-SHA-512
     * 5、PLAINTEXT（无认证）
     *
     * Kerberos 和 OAuth 需要额外环境准备（krb5.conf、Keytab、OAuth 服务器）
     * 6、SASL/GSSAPI (Kerberos)
     * 7、SASL/OAUTHBEARER
     *
     * @param properties properties
     */
    private static void putAuthConfig(Properties properties) {
        String authEnable = System.getProperty(KafkaConstants.MQ_KAFKA_AUTH_ENABLE, "false");
        boolean authFlag = Boolean.parseBoolean(authEnable);
        if (authFlag) {
            String defauleSaslMechanism = "PLAIN";
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            String saslMechanism = System.getProperty("mq.kafka.sasl.mechanism", defauleSaslMechanism);// 枚举：PLAINTEXT/SCRAM-SHA-256/SCRAM-SHA-512
            properties.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
            String loginModule = "org.apache.kafka.common.security.plain.PlainLoginModule";
            if (!saslMechanism.equals(defauleSaslMechanism)) {
                loginModule = "org.apache.kafka.common.security.scram.ScramLoginModule";
            }
            String user = "admin";
            String password = "admin-secret";

            String saslJaasConfig = getSaslJaasConfig(loginModule, user, password);
            properties.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        }
        /**
         * SSL
         */
        if (Boolean.getBoolean("kafka.ssl.enable")) {
            properties.put("security.protocol", "SSL");
            properties.put("ssl.truststore.location", "D:\\source-project\\mq-test\\basic-test\\src\\main\\resources\\ssl\\kafka\\kafka.truststore.jks");
            properties.put("ssl.truststore.password", "trustPassword");
        }
    }

    private static String getSaslJaasConfig(String loginModule, String userName, String pw) {
        if (StringUtils.isEmpty(userName)) {
            throw new ConfigException("Config item 'userName' of kafka appender can't be empty when securityProtocol is 'SASL_PLAINTEXT'.");
        }
        if (StringUtils.isEmpty(pw)) {
            throw new ConfigException("Config item 'password' of kafka appender can't be empty when securityProtocol is 'SASL_PLAINTEXT'.");
        }
        return loginModule + " required username=\"" + userName + "\" password=\"" + pw + "\";";
    }


}
