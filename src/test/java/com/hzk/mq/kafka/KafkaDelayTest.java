package com.hzk.mq.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.hzk.mq.kafka.common.KafkaDispatchProducer;
import com.hzk.mq.kafka.config.KafkaConfig;
import com.hzk.mq.kafka.constant.KafkaConstants;
import com.hzk.mq.kafka.delay.KafkaDelayManager;
import com.hzk.mq.kafka.util.KafkaAdminUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;


/**
 * kafka延迟消息单测
 * 延迟等级：com.hzk.mq.support.delay.MetaTime
 */
public class KafkaDelayTest {

    // Map<value, 时间戳VO>
    private static Map<String, InnerDelayTimestampVO> VALUE_TIMESTAMPVO_MAP = new ConcurrentHashMap<>(32);

    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = loggerContext.getLogger("root");
        root.setLevel(Level.INFO);
    }

    private String targetTopic;
    private String value;
    private int delaySeconds;
    private long mistakeTimeMillis;
    private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Before
    public void before(){
        /**
         * 可修改项
         */
        System.setProperty(KafkaConstants.BOOTSTRAP_SERVERS, "localhost:9092");
        // 目标topic
        targetTopic = "delay_test";
        value = "12-01-delay1";
        /**
         * 延迟秒数，2h内任意秒
         * 小于5s则直接发送， KafkaDispatchProducer.send(org.apache.kafka.clients.producer.ProducerRecord<java.lang.String,java.lang.String>, int)
         */
        delaySeconds = 7;
        // 误差，毫秒数
        mistakeTimeMillis = 2000;

        // 创建topic
        KafkaAdminUtil.createTopic(targetTopic, 4, (short) 1);
    }

    @After
    public void after(){
        KafkaAdminUtil.stop();
        KafkaDelayManager.stop();
    }

    /**
     * 发送n秒的延迟消息，消费者n秒后收到，误差1s内
     */
    @Test
    public void delayTest() throws Exception{
        /**
         * 1、启动延迟管理器，KafkaDelayManager
         * 2、启动targetTopic消费者
         * 3、发送n秒延迟消息
         * 4、验证延迟误差
         */
        // 1、启动延迟管理器，KafkaDelayManager
        KafkaDelayManager.start();

        // 2、启动targetTopic消费者
        InnerKafkaConsumer innerKafkaConsumer = new InnerKafkaConsumer(targetTopic);
        new Thread(()->{
            innerKafkaConsumer.start();
        }, "delayKafkaConsumer_" + targetTopic).start();

        // 等10s等延迟消费者分组完毕
        Thread.currentThread().sleep(10 * 1000);
        System.err.println("等10s等延迟消费者分组完毕");

        // 3、发送n秒延迟消息
        ProducerRecord<String, String> record = new ProducerRecord<>(targetTopic, value);
        KafkaDispatchProducer.send(record, delaySeconds).get();
        String sendTime = df.format(new Date());
        System.err.println("sendTime=" + sendTime);

        InnerDelayTimestampVO delayTimestampVO = new InnerDelayTimestampVO();
        delayTimestampVO.sendTimestamp = System.currentTimeMillis();
        VALUE_TIMESTAMPVO_MAP.put(value, delayTimestampVO);

        // 4、验证延迟误差
        Thread.currentThread().sleep(delaySeconds * 1000);
        boolean isWaiting = true;
        while (isWaiting) {
            InnerDelayTimestampVO tempDelayTimestampVO = VALUE_TIMESTAMPVO_MAP.get(value);
            if (tempDelayTimestampVO.receiveTimestamp == 0) {
                Thread.currentThread().sleep(1000);
                continue;
            }
            long mistake = delayTimestampVO.getMistake() - delaySeconds * 1000;
            boolean flag = Math.abs(mistake) < mistakeTimeMillis;
            System.err.println("单测结果，延迟误差毫秒值=" + Math.abs(mistake) + ",mistakeTimeMillis=" + mistakeTimeMillis);
            Assert.assertTrue(flag);
            isWaiting = false;
        }

    }

    public static InnerDelayTimestampVO getDelayTimestampVO(String value){
        return VALUE_TIMESTAMPVO_MAP.get(value);
    }

    private static class InnerKafkaConsumer{
        private String topic;

        public InnerKafkaConsumer(String topic){
            this.topic = topic;
        }

        public void start(){
            Properties properties = KafkaConfig.getConsumerConfig();
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, "default_consumer_group_delay");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
            // 订阅topic
            consumer.subscribe(Collections.singleton(topic));
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                if (consumerRecords.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> record:consumerRecords) {
                    String value = record.value();
                    InnerDelayTimestampVO delayTimestampVO = getDelayTimestampVO(value);
                    delayTimestampVO.receiveTimestamp = System.currentTimeMillis();
                    long mistake = delayTimestampVO.getMistake();
                    System.err.println("value=" + value
                            + ",sendTimestamp=" + delayTimestampVO.sendTimestamp
                            + ",receiveTimestamp=" + delayTimestampVO.receiveTimestamp
                            + ",mistake=" + mistake);
                }
                // 提交偏移量
                consumer.commitSync();
            }
        }
    }

    private static class InnerDelayTimestampVO {
        long sendTimestamp;
        long receiveTimestamp;

        public long getMistake(){
            return receiveTimestamp - sendTimestamp;
        }
    }


}
