package com.hzk.mq.kafka.util;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaAdminUtil {

    private static AdminClient ADMIN_CLIENT;

    static {

        // 本地
//        System.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        // 虚机
//        System.setProperty("bootstrap.servers", "172.20.158.201:9092,172.20.158.201:9093,172.20.158.201:9094");


        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("bootstrap.servers"));
        properties.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
//        properties.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "4000");




        // 认证
//        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
//        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
//        properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username='admin' password='admin-secret';");
        ADMIN_CLIENT = AdminClient.create(properties);
    }

    private KafkaAdminUtil(){

    }

    /**
     * 创建topic
     * @param topicName topicName
     * @param numPartitions 分区数，kafka默认4
     * @param replicationFactor 副本数，不能超过broker数
     * @return
     */
    public static boolean createTopic(String topicName, int numPartitions, short replicationFactor){
        if (isTopicExist(topicName)) {
            return true;
        }
        AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        CreateTopicsResult result = ADMIN_CLIENT.createTopics(
                Collections.singleton(new NewTopic(topicName, numPartitions, replicationFactor)));
        Map<String, KafkaFuture<Void>> values = result.values();
        values.forEach((name, future) ->{
            future.whenComplete((action, throwable) ->{
                if (throwable != null) {
                    throwable.printStackTrace();
                }
                System.out.println("createTopicSuccess,topic=" + topicName);
                atomicBoolean.set(true);
                countDownLatch.countDown();
            });
        });
        try {
            countDownLatch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return atomicBoolean.get();
    }

    public static boolean isTopicExist(String topicName) {
        try {
            ADMIN_CLIENT.describeTopics(Collections.singletonList(topicName)).all().get();
            return true;
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }


    public static void main(String[] args) {
        String topic = "test33";
        boolean flag = createTopic(topic, 4, (short)2);
        System.out.println(flag);


    }


}
