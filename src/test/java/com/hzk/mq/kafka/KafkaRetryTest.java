package com.hzk.mq.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.hzk.mq.kafka.common.KafkaDispatchProducer;
import com.hzk.mq.kafka.config.KafkaConfig;
import com.hzk.mq.kafka.constant.KafkaConstants;
import com.hzk.mq.kafka.delay.KafkaDelayManager;
import com.hzk.mq.kafka.util.KafkaAdminUtil;
import com.hzk.mq.support.delay.DelayControlManager;
import com.hzk.mq.support.delay.MetaTime;
import com.hzk.mq.support.retry.MQRetryManager;
import com.hzk.mq.support.util.ClassCastUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * kafka消费重试单测
 * 延迟等级：com.hzk.mq.support.delay.MetaTime
 * 重试等级：延迟等级+2
 */
public class KafkaRetryTest {

    // Map<value, 重试次数>
    private static Map<String, Integer> VALUE_RETRYNUM_MAP = new ConcurrentHashMap<>();

    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = loggerContext.getLogger("root");
        root.setLevel(Level.INFO);
    }

    private String targetTopic;
    private String value;
    private String groupId;
    volatile boolean isDiscard;
    private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Before
    public void before(){
        /**
         * 可修改项
         */
        System.setProperty("bootstrap.servers", "localhost:9092");
        // 目标topic
        targetTopic = "retry_test";
        value = "12-15-retry-01";
        groupId = "default_consumer_group";
        // 最大消费重试次数,重试等级=延迟等级+2
        System.setProperty(KafkaConstants.RetryConstants.MQ_KAFKA_CONSUMER_RETRY_TIMES, "3");

        // 创建topic,普通topic,重试topic
        KafkaAdminUtil.createTopic(targetTopic, 4, (short) 1);
        String retryTopic = MQRetryManager.getRetryTopic(groupId);
        KafkaAdminUtil.createTopic(retryTopic, 4, (short) 1);
    }

    @After
    public void after(){
        KafkaAdminUtil.stop();
        KafkaDelayManager.stop();
    }

    /**
     * 发送普通消息，消费者默认消费失败，查看同一条消息接收到多少次
     */
    @Test
    public void retryTest() throws Exception{
        /**
         * 1、启动延迟管理器，KafkaDelayManager
         * 2、启动targetTopic消费者
         * 3、发送普通消息
         * 4、验证接收次数
         */
        // 1、启动延迟管理器，KafkaDelayManager
        KafkaDelayManager.start();
        // 2、启动targetTopic消费者
        KafkaRetryTest.InnerKafkaConsumer innerKafkaConsumer = new KafkaRetryTest.InnerKafkaConsumer(this, targetTopic, groupId);
        new Thread(()->{
            innerKafkaConsumer.start();
        }, "retryKafkaConsumer_" + targetTopic).start();

        // 等10s等延迟消费者分组完毕
        Thread.currentThread().sleep(10 * 1000);
        System.err.println("等10s等延迟消费者分组完毕");

        // 3、发送普通消息
        ProducerRecord<String, String> record = new ProducerRecord<>(targetTopic, value);
        KafkaDispatchProducer.send(record).get();
        System.err.println("sendTime=" + df.format(new Date()));

        // 4、验证接收次数
        while (!isDiscard) {
            Thread.currentThread().sleep(1000);
        }
        int maxRetryNums = Integer.parseInt(System.getProperty(KafkaConstants.RetryConstants.MQ_KAFKA_CONSUMER_RETRY_TIMES));
        Integer retryNums = VALUE_RETRYNUM_MAP.get(value);
        System.err.println("value=" + value + ",retryNums=" + retryNums + ",maxRetryNums=" + maxRetryNums);
        boolean flag = maxRetryNums == retryNums;
        Assert.assertTrue(flag);
        System.err.println("KafkaRetryTest测试通过");
    }


    public static Integer retryNumsCount(String value, String topic){
        Integer retryNums = VALUE_RETRYNUM_MAP.get(value);
        if (retryNums == null) {
            retryNums = 0;
        } else {
            retryNums = retryNums +1;
        }
        VALUE_RETRYNUM_MAP.put(value, retryNums);
        System.err.println("接收时间=" + df.format(new Date()) + ",重试统计:" + VALUE_RETRYNUM_MAP + ",topic=" + topic);
        return retryNums;
    }



    private static class InnerKafkaConsumer{
        private KafkaRetryTest kafkaRetryTest;
        private String topic;
        private String groupId;
        private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        public InnerKafkaConsumer(KafkaRetryTest kafkaRetryTest, String topic, String groupId){
            this.kafkaRetryTest = kafkaRetryTest;
            this.topic = topic;
            this.groupId = groupId;
        }

        public void start(){
            // 创建topic
            boolean isCreateTopic = KafkaAdminUtil.createTopic(topic, 4, (short) 1);;
            if (!isCreateTopic) {
                System.err.println("createTopic error,topic=" + topic);
                return;
            }
            Properties properties = KafkaConfig.getConsumerConfig();
            String groupId = "default_consumer_group";
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
            // 订阅topic
            String retryTopic = MQRetryManager.getRetryTopic(groupId);
            List<String> topicList = new ArrayList<>();
            Collections.addAll(topicList, topic, retryTopic);
            consumer.subscribe(topicList);


            int maxRetryNums = Integer.parseInt(System.getProperty(KafkaConstants.RetryConstants.MQ_KAFKA_CONSUMER_RETRY_TIMES));
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                if (consumerRecords.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> record:consumerRecords) {
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

                    // 默认消费失败,转发至延迟队列
                    // 先消费逻辑,再处理消费失败场景
                    // doBizWorkFirst,无此方法，仅代表先走业务代码
                    System.err.println("doBizWorkFirst...");


                    // 统计重试次数
                    retryNumsCount(value, record.topic());


                    retryNums = retryNums + 1;
                    if (retryNums > maxRetryNums) {
                        // 超过最大重试次数，丢弃该消息
                        System.err.println("接收时间=" + df.format(new Date()) + ",重试次数=" + retryNums + ",大于最大重试次数" + maxRetryNums + ",丢弃消息,value=" + record.value());
                        kafkaRetryTest.isDiscard = true;
                    } else {
                        try {
                            dispatchToRetryTopic(topic, retryTopic, value, retryNums);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                }
                // 提交偏移量
                consumer.commitSync();
            }
        }

        private void dispatchToRetryTopic(String topic, String retryTopic, String value, int retryNums)
                throws InterruptedException, java.util.concurrent.ExecutionException {
            String delayTopic = MQRetryManager.getDelayTopic(retryNums);
            ProducerRecord<String, String> delayRecord = new ProducerRecord<>(delayTopic, value);
            delayRecord.headers().add(KafkaConstants.DelayConstants.ORIGIN_TOPIC, topic.getBytes(StandardCharsets.UTF_8));
            delayRecord.headers().add(KafkaConstants.DelayConstants.TARGET_TOPIC, retryTopic.getBytes(StandardCharsets.UTF_8));
            long startDeliverTime = DelayControlManager.getStartDeliverTimeByLevel(retryNums + 2);
            delayRecord.headers().add(KafkaConstants.DelayConstants.START_DELIVER_TIME, ClassCastUtil.longToBytes(startDeliverTime));
            delayRecord.headers().add(KafkaConstants.RetryConstants.RETRY_TIMES, ClassCastUtil.int2bytes(retryNums));
            KafkaDispatchProducer.send(delayRecord).get();
        }
    }



}
