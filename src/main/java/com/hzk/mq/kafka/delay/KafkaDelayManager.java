package com.hzk.mq.kafka.delay;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.hzk.mq.kafka.common.KafkaConsumerWorkerPool;
import com.hzk.mq.kafka.common.KafkaDispatchProducer;
import com.hzk.mq.kafka.config.KafkaConfig;
import com.hzk.mq.kafka.constant.KafkaConstants;
import com.hzk.mq.kafka.util.KafkaAdminUtil;
import com.hzk.mq.support.delay.DelayControlManager;
import com.hzk.mq.support.delay.MetaTime;
import com.hzk.mq.support.util.ClassCastUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * kafka延迟功能管理
 * 1、启动17个延迟等级的消费者，每个消费者1s拉取一次消息
 */
public class KafkaDelayManager {

    static {
        // 本地
        System.setProperty("bootstrap.servers", "localhost:9092");
//        System.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");

        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = loggerContext.getLogger("root");
        root.setLevel(Level.INFO);
    }

    private static Map<String, KafkaDelayConsumer> DELAY_CONSUMER_MAP = new HashMap<>(32);


    private static void initDelayEnv(){
        // 1、获取17个延迟等级的topicName集合
        List<String> delayTopicList = KafkaDelayManager.getDelayTopicList();
        // 2、创建topic，1个分区，2个副本
        for(String tempTopic:delayTopicList) {
            KafkaAdminUtil.createTopic(tempTopic, 1, (short) 1);
        }
        CountDownLatch countDownLatch = new CountDownLatch(delayTopicList.size());
        // 3、启动延迟队列消费者
        for(int i = 0; i < delayTopicList.size(); i++) {
            String delayTopic = delayTopicList.get(i);
            MetaTime metaTime = MetaTime.genInstanceByLevel(i+1);
            KafkaDelayConsumer kafkaDelayConsumer = new KafkaDelayConsumer(System.getProperty("bootstrap.servers"), delayTopic,
                    metaTime, countDownLatch);
            DELAY_CONSUMER_MAP.put(delayTopic, kafkaDelayConsumer);
            new Thread(()->{
                kafkaDelayConsumer.start();
            }, "kafkaDelayConsumer-" + delayTopic).start();
        }
        try {
            countDownLatch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.err.println("所有延迟消费者启动成功");

    }

    /**
     * 转发消息
     * @param messageRecord messageRecord
     * @return Future对象
     */
    public static Future<RecordMetadata> dispatchMessage(ProducerRecord<String, String> messageRecord){
        return KafkaDispatchProducer.send(messageRecord);
    }

    /**
     * 获取17个延迟等级的topicName集合
     * @return topicName集合
     */
    private static List<String> getDelayTopicList(){
        int[] supportMetaTimes = DelayControlManager.supportMetaTime;
        List<String> delayTopicList = new ArrayList<>(supportMetaTimes.length);
        for (int i = 0; i < supportMetaTimes.length; i++) {
            MetaTime metaTime = MetaTime.genInstanceByLevel(i+1);
            String name = metaTime.getName();
            String topicName = getDelayTopicName(name);
            delayTopicList.add(topicName);
        }
        return delayTopicList;
    }

    public static String getDelayTopicName(String metaTimeName) {
        return metaTimeName + "_bos";
    }

    public static void main(String[] args) {
        initDelayEnv();
    }




}
class KafkaDelayConsumer {

    private String bootstratp;
    private String deplyTopic;
    private MetaTime metaTime;
    private CountDownLatch countDownLatch;
    private volatile boolean isRunning;
    // 单位，秒
    private int unitSeconds = 0;

    public KafkaDelayConsumer(String bootstratp, String deplyTopic, MetaTime metaTime, CountDownLatch countDownLatch){
        this.bootstratp = bootstratp;
        this.deplyTopic = deplyTopic;
        this.metaTime = metaTime;
        this.unitSeconds = metaTime.getMillis() / 1000;
        this.countDownLatch = countDownLatch;
    }

    public void start(){
        isRunning = true;
        Properties properties = KafkaConfig.getConsumerConfig();
        // 消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, deplyTopic);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 订阅topic
        consumer.subscribe(Collections.singleton(deplyTopic));
        countDownLatch.countDown();

        String threadName = Thread.currentThread().getName();
        while (isRunning) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            if (consumerRecords.isEmpty()) {
                continue;
            }
            int count = consumerRecords.count();
            System.err.println(threadName + ",拉取批次:" + count);
            for (ConsumerRecord<String, String> record:consumerRecords) {
                System.out.println("partition=" + record.partition() + ",value=" + record.value() + ",offset=" + record.offset());
                try {
                    String targetTopic = "";
                    // 开始投递时间点
                    byte[] startDeliverTimeByteArray = null;
                    long startDeliverTime = 0L;
                    byte[] retryNumsBytes = null;
                    Header[] headers = record.headers().toArray();
                    if (headers != null && headers.length > 0) {
                        for (Header header : headers) {
                            String key = header.key();
                            if (key.equals(KafkaConstants.DelayConstants.TARGET_TOPIC)) {
                                targetTopic = new String(header.value());
                            } else if(key.equals(KafkaConstants.DelayConstants.START_DELIVER_TIME)){
                                startDeliverTimeByteArray = header.value();
                                startDeliverTime = ClassCastUtil.byteArrayToLong(startDeliverTimeByteArray);
                            } else if(key.equals(KafkaConstants.RetryConstants.RETRY_NUMBERS)){
                                retryNumsBytes = header.value();
                            }
                        }
                    }
                    int remainSeconds = (int) (startDeliverTime - System.currentTimeMillis()) / 1000;
                    String nextTopic = "";
                    if (remainSeconds > 0) {
                        if (remainSeconds >= unitSeconds) {
                            // 开始投递时间-当前时间 >= 当前单位，队首延迟时间未到，回滚偏移量
                            Thread.currentThread().sleep(1000);
                            System.err.println("seekCurrTime:" + System.currentTimeMillis() + ",回滚偏移量,topic:" + record.topic() + ",offset=" + record.offset());
                            TopicPartition topicPartition = new TopicPartition(deplyTopic, record.partition());
                            consumer.seek(topicPartition, record.offset());
                            continue;
                        }
                        // 开始投递时间-当前时间 < 当前单位，转发消息到低等级延迟topic，提交偏移量
                        MetaTime metaTime = DelayControlManager.selectMaxMetaTime(remainSeconds);
                        if (metaTime == MetaTime.delay_1s && unitSeconds == 1) {
                            Thread.currentThread().sleep(1000);
                            System.err.println("delay1s,seekCurrTime:" + System.currentTimeMillis() + ",回滚偏移量,topic:" + record.topic() + ",offset=" + record.offset());
                            TopicPartition topicPartition = new TopicPartition(deplyTopic, record.partition());
                            consumer.seek(topicPartition, record.offset());
                            continue;
                        }
                        String delayTopicName = KafkaDelayManager.getDelayTopicName(metaTime.getName());
                        nextTopic = delayTopicName;
                        dispatchMessage(targetTopic, record.value(), nextTopic, startDeliverTimeByteArray, retryNumsBytes);
                    } else if(unitSeconds == 1){
                        dispatchMessage(targetTopic, record.value(), targetTopic, startDeliverTimeByteArray, retryNumsBytes);
                    }
                    // Map<分区，偏移量>
                    Map<TopicPartition, OffsetAndMetadata> partitionOffSetMap = new HashMap<>();
                    partitionOffSetMap.put(new TopicPartition(deplyTopic, record.partition()), new OffsetAndMetadata(record.offset() + 1));
                    consumer.commitSync(partitionOffSetMap);
                    System.err.println("commitCurrTime:" + System.currentTimeMillis() + ",提交偏移量,topic:" + record.topic() + ",offset=" + (record.offset() + 1));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void dispatchMessage(String targetTopic, String value, String nextTopic, byte[] startDeliverTimeByteArray, byte[] retryNumsBytes)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        ProducerRecord<String, String> nextRecord = new ProducerRecord<>(nextTopic, value);
        // 目标topic
        nextRecord.headers().add(KafkaConstants.DelayConstants.TARGET_TOPIC, targetTopic.getBytes());
        // 开始投递时间
        nextRecord.headers().add(KafkaConstants.DelayConstants.START_DELIVER_TIME, startDeliverTimeByteArray);
        // 消费重试次数
        nextRecord.headers().add(KafkaConstants.RetryConstants.RETRY_NUMBERS, retryNumsBytes);
        KafkaDelayManager.dispatchMessage(nextRecord).get();
    }

    public void stop(){
        isRunning = false;
    }



}