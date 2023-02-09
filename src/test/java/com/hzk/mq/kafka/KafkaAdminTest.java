package com.hzk.mq.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.hzk.mq.kafka.config.KafkaConfig;
import com.hzk.mq.kafka.constant.KafkaConstants;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaAdminTest {

    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = loggerContext.getLogger("root");
        root.setLevel(Level.INFO);
    }

    private String targetTopic;
    private String value;
    private AdminClient adminClient;


    @Before
    public void before(){
        /**
         * 可修改项
         */
        System.setProperty(KafkaConstants.BOOTSTRAP_SERVERS, "localhost:9092");
        // 目标topic
        targetTopic = "sensor";
        value = "11-22-testvalue1";

        adminClient = AdminClient.create(KafkaConfig.getAdminConfig());
    }

    @After
    public void after(){
        adminClient.close();
    }


    @Test
    public void createTopicTest() throws Exception {
        AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        CreateTopicsResult result = adminClient.createTopics(
                Collections.singleton(new NewTopic(targetTopic, 4, (short) 1)));
        Map<String, KafkaFuture<Void>> values = result.values();
        values.forEach((name, future) ->{
            future.whenComplete((action, throwable) ->{
                if (throwable != null) {
                    throwable.printStackTrace();
                }
                System.out.println("createTopicSuccess,topic=" + targetTopic);
                atomicBoolean.set(true);
                countDownLatch.countDown();
            });
        });
        try {
            countDownLatch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        boolean createTopicFlag = atomicBoolean.get();
        System.out.println("创建topic,flag=" + createTopicFlag);
    }

    /**
     * 获取全部topic
     * @throws Exception
     */
    @Test
    public void listTopicsTest() throws Exception {
        System.err.println("------------------------------");
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> topicNameSet = listTopicsResult.names().get();
        topicNameSet.stream().forEach(System.out::println);
        System.err.println("------------------------------");
        Collection<TopicListing> topicListings = listTopicsResult.listings().get();
        topicListings.stream().forEach(e->{
            System.out.println(e);
        });
        System.err.println("------------------------------");
    }

    /**
     * 获取topic的描述信息
     * @throws Exception
     */
    @Test
    public void describeTopicsTest() throws Exception {
        System.err.println("------------------------------");
        String topicName = "test1";
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton(topicName));
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        stringTopicDescriptionMap.forEach((topic, describe) ->{
            System.out.println("topic=" + topic + ",describe=" + describe);
        });
        System.err.println("------------------------------");
    }

    /**
     * 获取topic的配置信息
     * @throws Exception
     */
    @Test
    public void describeConfigsTest() throws Exception {
        System.err.println("------------------------------");
        Set<String> topicNameSet = new HashSet<>();
        topicNameSet.add("delay_5s_bos");

        List<ConfigResource> configResourceList = new ArrayList<>();
        topicNameSet.stream().forEach(tempTopic ->{
            configResourceList.add(new ConfigResource(ConfigResource.Type.TOPIC, tempTopic));
        });
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(configResourceList);
        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
        configResourceConfigMap.forEach((configResource, config) ->{
            System.out.println("configResource=" + configResource + ",config=" + config);
        });
        System.err.println("------------------------------");
    }

    /**
     * 全部消费者组
     * @throws Exception
     */
    @Test
    public void listConsumerGroupsTest() throws Exception {
        System.err.println("------------------------------");
        Collection<ConsumerGroupListing> consumerGroupListings = adminClient.listConsumerGroups().all().get();
        consumerGroupListings.forEach((consumerGroupListing) ->{
            System.out.println("consumerGroupListing=" + consumerGroupListing);
            String groupId = consumerGroupListing.groupId();
            ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = adminClient.listConsumerGroupOffsets(groupId);
            KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> mapKafkaFuture = listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata();
            try {
                Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = mapKafkaFuture.get();
                System.out.println(topicPartitionOffsetAndMetadataMap);
            } catch (Exception e) {
                e.printStackTrace();
            }

        });
        System.err.println("------------------------------");
    }


}
