package com.hzk.mq.kafka.consumer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.hzk.mq.kafka.common.KafkaConsumerWorkerPool;
import com.hzk.mq.kafka.config.KafkaConfig;
import com.hzk.mq.kafka.constant.KafkaConstants;
import com.hzk.mq.kafka.offset.KafkaPartitionOffsetManager;
import com.hzk.mq.kafka.util.KafkaAdminUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class KafkaMultiTopicConsumerMain {

    private static String CONSUMER_RESULT_SUCCESS = "success";

    private static String CONSUMER_RESULT_LATER = "later";

    static {
        // 本地
        System.setProperty("bootstrap.servers", "localhost:9092");
//        System.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        // 虚机
//        System.setProperty("bootstrap.servers", "172.20.158.201:9092,172.20.158.201:9093,172.20.158.201:9094");

        System.setProperty("kafka.consumer.safe.enable", "true");

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
        String topic = "test1";
        String topic2 = "test2";
        Map<String, Integer> topic2concurrencyMap = new HashMap<>();
        topic2concurrencyMap.put(topic, 2);
        topic2concurrencyMap.put(topic2, 4);

        List<String> topicList = new ArrayList<>();
        topicList.add(topic);
//        topicList.add(topic2);

        for (String tempTopic : topicList) {
            /**
             * 1、消费者订阅前，先创建partition=4的topic
             */
            boolean isCreateTopic = KafkaAdminUtil.createTopic(topic, 4, (short) 1);
            if (!isCreateTopic) {
                System.err.println("createTopic error,topic=" + topic);
                return;
            }
            // 注册内存队列
            KafkaPartitionOffsetManager.registQueue(tempTopic, groupName);
        }
        // 订阅会修改线程id
//        consumer.subscribe(Collections.singleton(topic));
        consumer.subscribe(topicList);

        ReentrantLock lock = new ReentrantLock();
        // 创建拉取线程
        new PollMessageToDispatchThread(topic2concurrencyMap, consumer, groupName, lock).start();
        while (true) {
            for (String tempTopic : topicList) {
                KafkaPartitionOffsetManager.TopicOffsetMetadata topicOffsetMetadata = KafkaPartitionOffsetManager.poll(tempTopic, groupName, 10, TimeUnit.MILLISECONDS);
                if (topicOffsetMetadata == null){
                    continue;
                }
                System.err.println("提交偏移量,topic=" + tempTopic + ",偏移量:" + topicOffsetMetadata.getPartitionOffsetMap());
                if (isSafeConsumer()) {
                    lock.lock();
                    consumer.commitSync(topicOffsetMetadata.getPartitionOffsetMap());
                    lock.unlock();
                } else {
                    consumer.commitSync(topicOffsetMetadata.getPartitionOffsetMap());
                }
            }
        }

    }

    public static boolean isSafeConsumer(){
        return Boolean.getBoolean("kafka.consumer.safe.enable");
    }


}
class PollMessageToDispatchThread extends Thread {

    // Map<topic, 并发度>
    private Map<String, Integer> topic2concurrencyMap;

    // Map<topic, 信号量>
    private Map<String, Semaphore> topic2semaphoreMap;

    private KafkaConsumer<String, String> consumer;

    private String groupName;

    private ReentrantLock lock;

    public PollMessageToDispatchThread(Map<String, Integer> topic2concurrencyMap, KafkaConsumer<String, String> consumer, String groupName, ReentrantLock lock){
        this.topic2concurrencyMap = topic2concurrencyMap;
        this.consumer = consumer;
        this.groupName = groupName;
        this.lock = lock;

        topic2semaphoreMap = new HashMap<>(topic2concurrencyMap.size());
        for (Map.Entry<String, Integer> entry : topic2concurrencyMap.entrySet()) {
            topic2semaphoreMap.put(entry.getKey(), new Semaphore(entry.getValue()));
        }
    }

    @Override
    public void run(){

        while (true) {
            ConsumerRecords<String, String> consumerRecords;

            if (KafkaMultiTopicConsumerMain.isSafeConsumer()) {
                lock.lock();
                consumerRecords = consumer.poll(Duration.ofMillis(100));
                lock.unlock();
            } else {
                consumerRecords = consumer.poll(Duration.ofMillis(100));
            }

//            Set<TopicPartition> assignment = consumer.assignment();
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
            int maxRetryTime = Integer.parseInt(System.getProperty(KafkaConstants.RetryConstants.MQ_KAFKA_CONSUMER_RETRY_TIMES, "10"));

            for (ConsumerRecord<String, String> record:consumerRecords) {
                int partition = record.partition();
                System.out.println("topic=" + record.topic() + ",partition=" + record.partition() + ",value=" + record.value() + ",offset=" + record.offset());
//                semaphore.acquire();
                // 使用线程池执行业务
                KafkaConsumerWorkerPool.execute(() -> {
                    try {
                        String threadName = Thread.currentThread().getName();
//                        System.out.println(threadName + ",bizWork start,partition=" + partition);
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
                        partitionOffSetMap.put(new TopicPartition(record.topic(), partition), new OffsetAndMetadata(record.offset() + 1));
                        KafkaPartitionOffsetManager.offer(record.topic(), groupName, partitionOffSetMap);

//                        System.out.println(threadName + ",bizWork end,partition=" + partition);
//                        System.out.println("-----------------------------");
                    } finally {
//                        semaphore.release();
                    }
                });
            }
        }



    }

    private boolean doBizWork(String value){
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
