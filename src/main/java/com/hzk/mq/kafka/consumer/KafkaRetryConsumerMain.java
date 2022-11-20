package com.hzk.mq.kafka.consumer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.hzk.mq.kafka.common.KafkaConsumerWorkerPool;
import com.hzk.mq.kafka.common.KafkaDispatchProducer;
import com.hzk.mq.kafka.config.KafkaConfig;
import com.hzk.mq.kafka.constant.KafkaConstants;
import com.hzk.mq.kafka.util.KafkaAdminUtil;
import com.hzk.mq.support.delay.DelayControlManager;
import com.hzk.mq.support.retry.MQRetryManager;
import com.hzk.mq.support.util.ClassCastUtil;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * kafka消费重试
 */
public class KafkaRetryConsumerMain {

    private static String CONSUMER_RESULT_SUCCESS = "success";

    private static String CONSUMER_RESULT_LATER = "later";

    static {
        // 本地
        System.setProperty("bootstrap.servers", "localhost:9092");
//        System.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        // 虚机
//        System.setProperty("bootstrap.servers", "172.20.158.201:9092,172.20.158.201:9093,172.20.158.201:9094");

        // 最大消费重试次数
        System.setProperty(KafkaConstants.RetryConstants.CONSUMER_MAX_RETRY_NUMBERS, "3");

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
        String topic = "retry_test";
        String retryTopic = MQRetryManager.getRetryTopic(groupName);

        KafkaAdminUtil.createTopic(topic, 4, (short) 1);
        KafkaAdminUtil.createTopic(retryTopic, 4, (short) 1);

        consumer.subscribe(Arrays.asList(topic, retryTopic));

        // 最大消费重试次数
        int maxRetryNums = getConsumerMaxRetryNums();
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            if (consumerRecords.isEmpty()) {
                continue;
            }
            int count = consumerRecords.count();
            for (ConsumerRecord<String, String> record:consumerRecords) {
                int partition = record.partition();
                String value = record.value();
                System.err.println("receiveCurrTime=" + System.currentTimeMillis() + ",topic:" + record.topic() +
                        ",partition=" + record.partition() + ",value=" + record.value() + ",offset=" + record.offset());

                // TODO 默认消费失败，转发至延迟队列
                // 先消费逻辑，再处理消费失败场景


                int retryNums = 0;
                Header[] headers = record.headers().toArray();
                if (headers != null && headers.length > 0) {
                    for (Header header : headers) {
                        String key = header.key();
                        if (key.equals(KafkaConstants.RetryConstants.RETRY_NUMBERS)) {
                            retryNums = ClassCastUtil.bytes2Int(header.value());
                        }
                    }
                }
                System.err.println("重试次数:" + retryNums);
                if (retryNums == 0) {
                    // 第一次消费失败
                    retryNums = 1;
                    dispatchToRetryTopic(retryTopic, value, retryNums);
                } else {
                    if (retryNums > maxRetryNums) {
                        // 超过最大重试次数，丢弃该消息
                        System.err.println("丢弃消息...");
                        continue;
                    } else {
                        // 第n次消费失败
                        retryNums = retryNums + 1;
                        dispatchToRetryTopic(retryTopic, value, retryNums);
                    }
                }
                // Map<分区，偏移量>
                Map<TopicPartition, OffsetAndMetadata> partitionOffSetMap = new HashMap<>();
                partitionOffSetMap.put(new TopicPartition(topic, partition), new OffsetAndMetadata(record.offset() + 1));
                consumer.commitSync(partitionOffSetMap);
                System.err.println("提交偏移量:" + partitionOffSetMap);
            }
        }

    }

    private static void dispatchToRetryTopic(String retryTopic, String value, int retryNums)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        String delayTopic = MQRetryManager.getDelayTopic(retryNums);
        ProducerRecord<String, String> delayRecord = new ProducerRecord<>(delayTopic, value);
        delayRecord.headers().add(KafkaConstants.DelayConstants.TARGET_TOPIC, retryTopic.getBytes(StandardCharsets.UTF_8));
        long startDeliverTime = DelayControlManager.getStartDeliverTimeByLevel(retryNums + 2);
        delayRecord.headers().add(KafkaConstants.DelayConstants.START_DELIVER_TIME, ClassCastUtil.longToBytes(startDeliverTime));
        delayRecord.headers().add(KafkaConstants.RetryConstants.RETRY_NUMBERS, ClassCastUtil.int2bytes(retryNums));
        KafkaDispatchProducer.send(delayRecord).get();
    }

    private static int getConsumerMaxRetryNums(){
        return Integer.getInteger(KafkaConstants.RetryConstants.CONSUMER_MAX_RETRY_NUMBERS);
    }


}
