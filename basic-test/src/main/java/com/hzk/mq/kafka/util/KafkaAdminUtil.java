package com.hzk.mq.kafka.util;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.hzk.mq.kafka.config.KafkaConfig;
import com.hzk.mq.kafka.constant.KafkaConstants;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaAdminUtil {

    private static AdminClient ADMIN_CLIENT;

    private static final Object LOCKER = new Object();

    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = loggerContext.getLogger("root");
        root.setLevel(Level.INFO);
    }

    private KafkaAdminUtil(){

    }

    private static void initAdminClient(){
        if (ADMIN_CLIENT == null) {
            synchronized (LOCKER) {
                if (ADMIN_CLIENT == null) {
                    ADMIN_CLIENT = AdminClient.create(KafkaConfig.getAdminConfig());
                }
            }
        }
    }

    public static AdminClient getAdminClient(){
        initAdminClient();
        return ADMIN_CLIENT;
    }

    /**
     * 创建topic
     * @param topicName topicName
     * @param numPartitions 分区数，kafka默认1
     * @param replicationFactor 副本数，不能超过broker数
     * @return 布尔值
     */
    public static boolean createTopic(String topicName, int numPartitions, short replicationFactor){
        initAdminClient();
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


    public static boolean deleteTopic(String topicName){
        initAdminClient();
        DeleteTopicsOptions deleteTopicsOptions = new DeleteTopicsOptions().timeoutMs(1000 * 10);
        DeleteTopicsResult deleteTopicsResult = ADMIN_CLIENT.deleteTopics(Collections.singleton(topicName), deleteTopicsOptions);
        Map<String, KafkaFuture<Void>> stringKafkaFutureMap = deleteTopicsResult.topicNameValues();
        KafkaFuture<Void> voidKafkaFuture = stringKafkaFutureMap.get(topicName);
        try {
            Void aVoid = voidKafkaFuture.get();
            System.out.println(aVoid);
        } catch (Exception e) {

        }
        return true;
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

    public static void stop(){
        ADMIN_CLIENT.close();
        ADMIN_CLIENT = null;
    }


    public static void main(String[] args) throws Exception{
//        System.setProperty(KafkaConstants.BOOTSTRAP_SERVERS, "localhost:9092");
        initAdminClient();

        ListTopicsResult listTopicsResult = ADMIN_CLIENT.listTopics();
        Collection<TopicListing> topicListingCollection = listTopicsResult.listings().get();
        List<String> topicList = new ArrayList<>(topicListingCollection.size());
        topicListingCollection.stream().forEach(e ->{
            String topic = e.name();
            System.out.println(topic + ",internal=" + e.isInternal());
            topicList.add(topic);
        });
        DescribeTopicsResult describeTopicsResult = ADMIN_CLIENT.describeTopics(topicList);
        describeTopicsResult.values().forEach((k,v)->{
            System.out.println(k);
            try {
                TopicDescription topicDescription = v.get();
                System.out.println(topicDescription);
            } catch (Exception e) {

            }

        });


//        DescribeClusterResult describeClusterResult = ADMIN_CLIENT.describeCluster();
//        System.err.println(describeClusterResult.controller().get());

        Collection<ConsumerGroupListing> groupListingCollection = ADMIN_CLIENT.listConsumerGroups().all().get();
        groupListingCollection.stream().forEach(e ->{
            String groupId = e.groupId();
            System.out.println(groupId);
            try {
                DescribeConsumerGroupsResult descConsumerGroup = ADMIN_CLIENT.describeConsumerGroups(Arrays.asList(groupId));


                Map<TopicPartition, OffsetAndMetadata> partitionOffsetAndMetadataMap = ADMIN_CLIENT.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata().get();
                System.out.println(partitionOffsetAndMetadataMap);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });


        String topic = "test";
        boolean flag = createTopic(topic, 4, (short)1);
        System.err.println("createTopic:" + flag);
        // 会导致服务端宕机
//        flag = deleteTopic(topic);
//        System.out.println(flag);

        listTopicsResult = ADMIN_CLIENT.listTopics();
//        DescribeTopicsResult describeTopicsResult = ADMIN_CLIENT.describeTopics(Collections.singleton(topic));

        stop();
    }


}
