package com.hzk.mq.kafka.consumer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.hzk.mq.kafka.common.KafkaConsumerWorkerPool;
import com.hzk.mq.kafka.config.KafkaConfig;
import com.hzk.mq.kafka.constant.KafkaConstants;
import com.hzk.mq.kafka.util.KafkaAdminUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 开源版简单消费者
 * 1、消费者订阅前，先创建partition=4的topic
 * 2、mq并发度。节点内部线程池并发
 * 3、消费重试
 *      while(n)调动业务MessageListener，仍返回拒绝，则丢弃消息。 n暂设为10
 */
public class KafkaConsumerMain {


    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = loggerContext.getLogger("root");
        root.setLevel(Level.INFO);
    }

    public static void main(String[] args) throws Exception{
        // 本地
        System.setProperty(KafkaConstants.BOOTSTRAP_SERVERS, "localhost:9092");

        String topic = "test1";
        Properties properties = KafkaConfig.getConsumerConfig();
        // 消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "default_consumer_group");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        /**
         * 1、消费者订阅前，先创建partition=4的topic
         */
        boolean isCreateTopic = KafkaAdminUtil.createTopic(topic, 4, (short) 1);
        if (!isCreateTopic) {
            System.err.println("createTopic error,topic=" + topic);
            return;
        }
        // 并发度
        int concurrency = 2;
        Semaphore semaphore = new Semaphore(concurrency);

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // 订阅会修改线程id
        consumer.subscribe(Collections.singleton(topic));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            if (consumerRecords.isEmpty()) {
                continue;
            }
            int count = consumerRecords.count();
            System.err.println("拉取批次:" + count);
            CountDownLatch countDownLatch = new CountDownLatch(count);
            int maxRetryTime = Integer.parseInt(System.getProperty(KafkaConstants.RetryConstants.MQ_KAFKA_CONSUMER_RETRY_TIMES, "10"));
            for (ConsumerRecord<String, String> record:consumerRecords) {
                int partition = record.partition();
                System.err.println("receiveCurrTime=" + df.format(new Date()) + ",topic=" + record.topic() + ",value=" + record.value() +
                        ",partition=" + record.partition() + ",offset=" + record.offset());
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
                        System.out.println(threadName + ",bizWork end,partition=" + partition);
                        System.out.println("-----------------------------");
                    } finally {
                        semaphore.release();
                        countDownLatch.countDown();
                    }
                });
            }
            // 最多等待10分钟
            countDownLatch.await(10, TimeUnit.MINUTES);
            // 批量提交偏移量
            consumer.commitSync();
        }

    }

    private static boolean doBizWork(String value){
        try {
            // 模拟业务执行1s
            Thread.currentThread().sleep(1000 * 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }

}
