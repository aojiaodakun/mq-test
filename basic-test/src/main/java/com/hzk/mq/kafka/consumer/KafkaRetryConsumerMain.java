package com.hzk.mq.kafka.consumer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
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
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

/**
 * kafka消费重试
 * 基于延迟消息实现
 * 1、启动KafkaDelayManager
 * 2、启动KafkaRetryConsumerMain
 * 3、启动KafkaRetryProducerMain
 */
public class KafkaRetryConsumerMain {

    private static String CONSUMER_RESULT_SUCCESS = "success";

    private static String CONSUMER_RESULT_LATER = "later";

    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = loggerContext.getLogger("root");
        root.setLevel(Level.INFO);
    }

    public static void main(String[] args) throws Exception{
        // 本地
        System.setProperty(KafkaConstants.BOOTSTRAP_SERVERS, "localhost:9092");
        // 最大消费重试次数
        System.setProperty(KafkaConstants.RetryConstants.MQ_KAFKA_CONSUMER_RETRY_TIMES, "3");
        String topic = "retry_test";

        Properties properties = KafkaConfig.getConsumerConfig();
        // 消费者组
        String groupName = "default_consumer_group";
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        String retryTopic = MQRetryManager.getRetryTopic(groupName);

        // 创建topic
        KafkaAdminUtil.createTopic(topic, 4, (short) 1);
        KafkaAdminUtil.createTopic(retryTopic, 4, (short) 1);
        // 订阅源topic，重试topic
        consumer.subscribe(Arrays.asList(topic, retryTopic));

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // 最大消费重试次数
        int maxRetryNums = getConsumerMaxRetryTimes();
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            if (consumerRecords.isEmpty()) {
                continue;
            }
            int count = consumerRecords.count();
            for (ConsumerRecord<String, String> record:consumerRecords) {
                int partition = record.partition();
                String value = record.value();

                int retryNums = 0;
                Header[] headers = record.headers().toArray();
                if (headers != null && headers.length > 0) {
                    for (Header header : headers) {
                        String key = header.key();
                        if (key.equals(KafkaConstants.RetryConstants.RETRY_TIMES)) {
                            retryNums = ClassCastUtil.bytes2Int(header.value());
                        }
                    }
                }
                // 第n次消费失败
                System.err.println("重试次数=" + retryNums + ",receiveCurrTime=" + df.format(new Date()) + ",topic=" + record.topic() +
                        ",partition=" + record.partition() + ",offset=" + record.offset() + ",value=" + record.value());

                // 默认消费失败,转发至延迟队列
                // 先消费逻辑,再处理消费失败场景
                // doBizWorkFirst,无此方法，仅代表先走业务代码
                System.err.println("doBizWorkFirst...");



                retryNums = retryNums + 1;
                if (retryNums > maxRetryNums) {
                    // 超过最大重试次数，丢弃该消息
                    System.err.println("重试次数=" + retryNums + ",receiveCurrTime=" + df.format(new Date()) + ",大于最大重试次数" + maxRetryNums + ",丢弃消息,value=" + record.value());
                } else {
                    dispatchToRetryTopic(retryTopic, value, retryNums);
                }
                // Map<分区，偏移量>
                Map<TopicPartition, OffsetAndMetadata> partitionOffSetMap = new HashMap<>();
                partitionOffSetMap.put(new TopicPartition(record.topic(), partition), new OffsetAndMetadata(record.offset() + 1));
                consumer.commitSync(partitionOffSetMap);
                System.err.println("提交偏移量:" + partitionOffSetMap);
                System.err.println("-----------------------------------");
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
        delayRecord.headers().add(KafkaConstants.RetryConstants.RETRY_TIMES, ClassCastUtil.int2bytes(retryNums));
        KafkaDispatchProducer.send(delayRecord).get();
    }

    private static int getConsumerMaxRetryTimes(){
        return Integer.getInteger(KafkaConstants.RetryConstants.MQ_KAFKA_CONSUMER_RETRY_TIMES);
    }


}
