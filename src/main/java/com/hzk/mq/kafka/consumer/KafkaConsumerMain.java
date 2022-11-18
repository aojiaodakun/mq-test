package com.hzk.mq.kafka.consumer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.hzk.mq.kafka.common.KafkaConsumerWorkerPool;
import com.hzk.mq.kafka.config.KafkaConfig;
import com.hzk.mq.kafka.util.KafkaAdminUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 1、消费者订阅前，先创建partition=4的topic
 * 2、mq并发度。节点内部线程池并发
 * 3、消费重试
 *      while(n)调动业务MessageListener，仍返回拒绝，则丢弃消息。 n暂设为10
 */
public class KafkaConsumerMain {

    private static String CONSUMER_RESULT_SUCCESS = "success";

    private static String CONSUMER_RESULT_LATER = "later";

    public KafkaConsumerMain(){

    }

    static {
        // 本地
        System.setProperty("bootstrap.servers", "localhost:9092");
//        System.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        // 虚机
//        System.setProperty("bootstrap.servers", "172.20.158.201:9092,172.20.158.201:9093,172.20.158.201:9094");

        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = loggerContext.getLogger("root");
        root.setLevel(Level.INFO);
    }

    public static void main(String[] args) throws Exception{
        Properties properties = KafkaConfig.getConsumerConfig();
        // 消费者组
        String groupName = "default_consumer_group";
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 订阅topic=test
        String topic = "delay_test";
        // 注册内存队列
//        PartitionOffsetManager.registQueue(topic, groupName);
        /**
         * 1、消费者订阅前，先创建partition=4的topic
         */
        boolean isCreateTopic = createTopic(topic);
        if (!isCreateTopic) {
            System.err.println("createTopic error,topic=" + topic);
            return;
        }
        // 并发度
        int concurrency = 2;
        Semaphore semaphore = new Semaphore(concurrency);

        // 订阅会修改线程id
        consumer.subscribe(Collections.singleton(topic));
        
        while (true) {
            // 默认32条
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            Set<TopicPartition> assignment = consumer.assignment();
//            if (assignment.size() > 0 && consumerRecords.isEmpty()) {
//                TopicPartition topicPartition = new TopicPartition(topic, 3);
//                consumer.seek(topicPartition, 2);
//                continue;
//            }
            if (consumerRecords.isEmpty()) {
                continue;
            }

            int count = consumerRecords.count();
            System.err.println("拉取批次:" + count);
            CountDownLatch countDownLatch = new CountDownLatch(count);
            int maxRetryTime = Integer.parseInt(System.getProperty("kafka.consumer.retry.time", "10"));
            for (ConsumerRecord<String, String> record:consumerRecords) {
                int partition = record.partition();
                System.err.println("receiveCurrTime=" + System.currentTimeMillis() + ",partition=" + record.partition() + ",consumer:" + record.key() + "--" + record.value() + ",offset=" + record.offset());
                semaphore.acquire();
                // 使用线程池执行业务
                KafkaConsumerWorkerPool.execute(() -> {
                    try {
                        String threadName = Thread.currentThread().getName();
                        System.out.println(threadName + ",bizWork start,partition=" + partition);
                        String value = record.value();
                        int failTime = 0;
                        while (failTime < maxRetryTime) {
                            boolean flag = doBizWork(value);
                            if (flag) {
                                break;
                            }
                            failTime++;
                        }
                        // Map<分区，偏移量>
                        Map<TopicPartition, OffsetAndMetadata> partitionOffSetMap = new HashMap<>();
                        partitionOffSetMap.put(new TopicPartition(topic, partition), new OffsetAndMetadata(record.offset() + 1));

//                            PartitionOffsetManager.offer(topic, groupName, partitionOffSetMap);
                        System.out.println(threadName + ",bizWork end,partition=" + partition);
                        System.out.println("-----------------------------");
                    } finally {
                        semaphore.release();
                        countDownLatch.countDown();
                    }
                });
            }
            // 4*32，最多等待10分钟
            countDownLatch.await(10, TimeUnit.MINUTES);
            // 提交偏移量
            consumer.commitSync();
//                while (true) {
//                    // 32
//                    PartitionOffsetManager.TopicOffsetMetadata topicOffsetMetadata = PartitionOffsetManager.poll(topic, groupName);
//                    if (topicOffsetMetadata == null){
//                        break;
//                    }
//                    System.err.println("提交偏移量,topic=" + topic + ",偏移量:" + topicOffsetMetadata.getPartitionOffsetMap());
//                    consumer.commitSync(topicOffsetMetadata.getPartitionOffsetMap());
//                }
        }


    }

    /**
     * 创建topic
     * @param topic topic
     * @return 布尔值
     */
    private static boolean createTopic(String topic){
        int createTopicLimit = 3;
        int createTopicTime = 0;
        while (createTopicTime < createTopicLimit) {
            /**
             * TODO，副本数要考虑集群节点
             * 1、节点数大于1，副本=2
             * 2、节点数=1，副本=1
             */
            boolean isCreateTopic = KafkaAdminUtil.createTopic(topic, 4, (short) 1);
            if (isCreateTopic) {
                return true;
            }
            createTopicTime++;
        }
        return false;
    }

    private static boolean doBizWork(String value){
        try {
            Thread.currentThread().sleep(1000 * 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        if (value.contains("5")) {
//            return false;
//        }
        return true;
    }

}
//
///**
// * KafkaConsumer是非线程安全的类，已
// */
//class CommitOffsetThread extends Thread {
//
//    // Map<topic_group,consumer>
//    private static Map<String, KafkaConsumer<String, String>> topicGroup2consumerMap = new ConcurrentHashMap<>();
//
////    private static ArrayBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> QUEUE = null;
//
//    private static ArrayBlockingQueue<TopicOffsetMetadata> QUEUE = null;
//
//    static {
//        int capacity = Integer.parseInt(System.getProperty("kafka.consumer.offset.queue.capacity", "1000"));
//        QUEUE = new ArrayBlockingQueue<>(capacity);
//    }
//
//    /**
//     * 注册消费者
//     * @param topic topic
//     * @param groupName groupName
//     * @param consumer consumer
//     */
//    public static void registConsumer(String topic, String groupName, KafkaConsumer<String, String> consumer){
//        topicGroup2consumerMap.put(topic + "_" + groupName, consumer);
//    }
//
//    public static boolean offer(String topic, String groupName, Map<TopicPartition, OffsetAndMetadata> partitionOffsetMap){
//        return QUEUE.offer(new TopicOffsetMetadata(topic, groupName, partitionOffsetMap));
//    }
//
//    private static class TopicOffsetMetadata {
//        String topic;
//        String groupName;
//        Map<TopicPartition, OffsetAndMetadata> partitionOffsetMap;
//
//        public TopicOffsetMetadata(String topic, String groupName, Map<TopicPartition, OffsetAndMetadata> partitionOffsetMap) {
//            this.topic = topic;
//            this.groupName = groupName;
//            this.partitionOffsetMap = partitionOffsetMap;
//        }
//
//        public String getTopic() {
//            return topic;
//        }
//
//        public String getGroupName() {
//            return groupName;
//        }
//
//        public Map<TopicPartition, OffsetAndMetadata> getPartitionOffsetMap() {
//            return partitionOffsetMap;
//        }
//    }
//
//    @Override
//    public void run() {
//        while (true) {
//            TopicOffsetMetadata topicOffsetMetadata = null;
//            try {
//                topicOffsetMetadata = QUEUE.take();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            String topic = topicOffsetMetadata.getTopic();
//            String groupName = topicOffsetMetadata.getGroupName();
//            String key = topic + "_" + groupName;
//            KafkaConsumer<String, String> consumer = topicGroup2consumerMap.get(key);
//            System.err.println("提交偏移量,key=" + key + ",偏移量:" + topicOffsetMetadata.getPartitionOffsetMap());
//            consumer.commitSync(topicOffsetMetadata.getPartitionOffsetMap());
//        }
//    }
//}
