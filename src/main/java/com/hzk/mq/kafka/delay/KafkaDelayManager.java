package com.hzk.mq.kafka.delay;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * kafka延迟功能管理
 * 1、启动18个延迟等级的消费者，每个消费者1s拉取一次消息
 */
public class KafkaDelayManager {

    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = loggerContext.getLogger("root");
        root.setLevel(Level.INFO);
    }
    private static Map<String, KafkaDelayConsumer> LEVEL_CONSUMER_MAP = new HashMap<>(2);

    private static Map<String, ReentrantLock> LEVEL_LOCK_MAP = new HashMap<>(2);

    // Map<level, topicList>
    private static Map<String, List<MetaTime>> LEVEL_DELAYTOPICS_MAP = new HashMap<>(2);
    // 低等级:1s~5m
    public static String DELAYTOPIC_LEVEL_LOW = "low";
    // 高等级:5m~2h
    public static String DELAYTOPIC_LEVEL_HIGH = "high";

    // TODO,测试用，一条数据经历过的延迟节点和拉取次数。Map<value, Map<topic, count>>
    private static Map<String, Map<String, Integer>> VALUE_TOPIC2COUNT_MAP = new ConcurrentHashMap<>(32);


    public static void start(){
        // 1、获取18个延迟等级的topicName集合
        List<String> delayTopicList = KafkaDelayManager.getDelayTopicList();
        // 2、创建topic，1个分区，2个副本
        for(String tempTopic:delayTopicList) {
            KafkaAdminUtil.createTopic(tempTopic, 1, (short) 1);
        }
        CountDownLatch countDownLatch = new CountDownLatch(2);
        // 3、启动延迟队列消费者
        initDelayConsumer(DELAYTOPIC_LEVEL_LOW, countDownLatch);
        initDelayConsumer(DELAYTOPIC_LEVEL_HIGH, countDownLatch);
        try {
            countDownLatch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.err.println("所有延迟消费者启动成功");
    }

    public static void stop(){
        LEVEL_CONSUMER_MAP.forEach((level,delayConsumer)->{delayConsumer.stop();});
    }

    private static void initDelayConsumer(String delayLevel, CountDownLatch countDownLatch){
        String delayThreadName = "kafkaDelayConsumer-" + delayLevel;
        List<MetaTime> metaTimeList = LEVEL_DELAYTOPICS_MAP.get(delayLevel);
        ReentrantLock lock = new ReentrantLock();
        KafkaDelayConsumer kafkaDelayConsumer = new KafkaDelayConsumer(delayLevel,
                metaTimeList, delayThreadName, countDownLatch, lock);
        LEVEL_CONSUMER_MAP.put(delayLevel, kafkaDelayConsumer);
        LEVEL_LOCK_MAP.put(delayLevel, lock);
        new Thread(()->{
            kafkaDelayConsumer.start();
        }, delayThreadName).start();
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
            List<MetaTime> levelTopicList;
            String level = DELAYTOPIC_LEVEL_LOW;
            if ((i+1) <= 9) {
                levelTopicList = LEVEL_DELAYTOPICS_MAP.get(level);
            } else {
                level = DELAYTOPIC_LEVEL_HIGH;
                levelTopicList = LEVEL_DELAYTOPICS_MAP.get(level);
            }
            if (levelTopicList == null) {
                levelTopicList = new ArrayList<>();
                LEVEL_DELAYTOPICS_MAP.put(level, levelTopicList);
            }
            levelTopicList.add(metaTime);
        }
        return delayTopicList;
    }


    public static String getDelayTopicName(String metaTimeName) {
        return metaTimeName + "_bos";
    }

    public static void raminDelayTopicCount(String value, String delayTopic){
        Map<String, Integer> delayTopic2countMap = VALUE_TOPIC2COUNT_MAP.get(value);
        if (delayTopic2countMap == null) {
            delayTopic2countMap = new HashMap<>();
            VALUE_TOPIC2COUNT_MAP.put(value, delayTopic2countMap);
        }
        if (!delayTopic2countMap.containsKey(delayTopic)) {
            delayTopic2countMap.put(delayTopic, 1);
        } else {
            Integer integer = delayTopic2countMap.get(delayTopic);
            integer = integer +1;
            delayTopic2countMap.put(delayTopic, integer);
        }
    }

    public static void printDelayTopicCount(String value){
        Map<String, Integer> delayTopic2countMap = VALUE_TOPIC2COUNT_MAP.get(value);
        if (delayTopic2countMap != null) {
            System.err.print("value=" + value + ",");
            System.err.println(delayTopic2countMap);
            System.out.println("------------------------------");
        }
    }

    public static void removeDelayTopicCount(String value){
        VALUE_TOPIC2COUNT_MAP.remove(value);
    }

    public static void main(String[] args) {
        System.setProperty("bootstrap.servers", "localhost:9092");
        start();
    }




}

class KafkaDelayConsumer {

    private KafkaConsumer<String, String> consumer;
    private String delayTopicLevel;
    private List<MetaTime> metaTimeList;
    private String groupId;
    private CountDownLatch countDownLatch;
    private ReentrantLock lock;
    private volatile boolean isRunning;
    // <topic, unitSeconds>
    private Map<String, Integer> topic2unitsecondsMap;



    public KafkaDelayConsumer(String delayTopicLevel, List<MetaTime> metaTimeList, String groupId, CountDownLatch countDownLatch, ReentrantLock lock){
        this.delayTopicLevel = delayTopicLevel;
        this.metaTimeList = metaTimeList;
        this.groupId = groupId;
        this.countDownLatch = countDownLatch;
        this.lock = lock;

        topic2unitsecondsMap = new HashMap<>(metaTimeList.size());
        for (MetaTime tempMetaTime : metaTimeList) {
            topic2unitsecondsMap.put(KafkaDelayManager.getDelayTopicName(tempMetaTime.getName()), tempMetaTime.getMillis()/1000);
        }
    }

    private List<String> getDelayTopicList(){
        List<String> topicList = new ArrayList<>(metaTimeList.size());
        metaTimeList.stream().forEach(metaTime -> topicList.add(KafkaDelayManager.getDelayTopicName(metaTime.getName())));
        return topicList;
    }



    private long getPollTimeoutMillis(){
        if (this.delayTopicLevel.equals(KafkaDelayManager.DELAYTOPIC_LEVEL_LOW)) {
            return Long.getLong("kafka.low.delayconsumer.poll.timeout", 500);
        } else {
            return Long.getLong("kafka.high.delayconsumer.poll.timeout", 1000);
        }
    }

    private long getUnexpiredRecordWaitMillis(){
        if (this.delayTopicLevel.equals(KafkaDelayManager.DELAYTOPIC_LEVEL_LOW)) {
            return Long.getLong("kafka.low.delayconsumer.poll.timeout", 500);
        } else {
            return Long.getLong("kafka.high.delayconsumer.poll.timeout", 4000);
        }
    }

    private ConsumerRecords<String, String> syncPoll(){
        try {
            lock.lock();
            return consumer.poll(Duration.ofMillis(getPollTimeoutMillis()));
        } finally {
            lock.unlock();
        }
    }


    public void start(){
        isRunning = true;
        Properties properties = KafkaConfig.getConsumerConfig();
        // 消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumer = new KafkaConsumer<>(properties);
        // 订阅topic
        consumer.subscribe(getDelayTopicList());
        countDownLatch.countDown();

        String threadName = Thread.currentThread().getName();
        while (isRunning) {
            ConsumerRecords<String, String> consumerRecords = syncPoll();
            if (consumerRecords.isEmpty()) {
                continue;
            }
            int count = consumerRecords.count();
//            System.err.println(threadName + ",拉取批次:" + count);
            for (ConsumerRecord<String, String> record:consumerRecords) {
//                System.out.println("partition=" + record.partition() + ",value=" + record.value() +
//                        ",offset=" + record.offset() + ",recordTimeStamp:" + record.timestamp() + ",recordTimeStampType:" + record.timestampType());

                String value = record.value();
//                KafkaDelayManager.printDelayTopicCount(value);

                try {
                    /**
                     * 获取消息头信息
                     * 1、目标topic
                     * 2、开始投递时间点
                     * 3、消费重试次数
                     */
                    Map<String, Object> recordHeaderMap = getRecordHeaderMap(record);
                    String originTopic = recordHeaderMap.get(KafkaConstants.DelayConstants.ORIGIN_TOPIC).toString();
                    String targetTopic = recordHeaderMap.get(KafkaConstants.DelayConstants.TARGET_TOPIC).toString();
                    byte[] startDeliverTimeByteArray = (byte[])recordHeaderMap.get(KafkaConstants.DelayConstants.START_DELIVER_TIME);
                    long startDeliverTime = ClassCastUtil.byteArrayToLong(startDeliverTimeByteArray);
                    byte[] retryNumsBytes = null;
                    if (recordHeaderMap.containsKey(KafkaConstants.RetryConstants.RETRY_TIMES)) {
                        retryNumsBytes = (byte[])recordHeaderMap.get(KafkaConstants.RetryConstants.RETRY_TIMES);
                    }

                    String deplyTopic = record.topic();
                    int unitSeconds = topic2unitsecondsMap.get(deplyTopic);

                    // TODO, 测试用
                    KafkaDelayManager.raminDelayTopicCount(value, deplyTopic);


                    // 每个消息只能存储停留单位时间秒数
                    long recordCreateTimestamp = record.timestamp();
                    int storeRemainSeconds = (int) ((recordCreateTimestamp + unitSeconds*1000) - System.currentTimeMillis()) / 1000;
                    // (创建时间 + 当前单位) - 当前时间 = 存储停留时间，存储停留时间>0=延迟时间未到，回滚偏移量
                    if (storeRemainSeconds > 0) {
//                        System.err.println("seekCurrTime:" + System.currentTimeMillis() + ",回滚偏移量,topic:" + record.topic() + ",offset=" + record.offset());
                        TopicPartition topicPartition = new TopicPartition(deplyTopic, record.partition());
                        consumer.seek(topicPartition, record.offset());
                        // 等待避免立即拉取下一批数据。
                        Thread.currentThread().sleep(getUnexpiredRecordWaitMillis());
                        // 单分区，队首消息延迟时间未到，其他消息均未到期，使用break而非continue
                        break;
                    }
                    // 延迟时间到，转发至低等级延迟topic或targetTopic
                    int remainSeconds = (int) (startDeliverTime - System.currentTimeMillis()) / 1000;
                    String nextTopic = "";
                    if (remainSeconds > 0) {
                        // 开始投递时间-当前时间 < 当前单位，转发消息到低等级延迟topic，提交偏移量
                        MetaTime metaTime = DelayControlManager.selectMaxMetaTime(remainSeconds);
                        if (metaTime == MetaTime.delay_1s && unitSeconds == 1) {
                            // 等待避免立即拉取下一批数据。
                            Thread.currentThread().sleep(500);
//                            System.err.println("delay1s,seekCurrTime:" + System.currentTimeMillis() + ",回滚偏移量,topic:" + record.topic() + ",offset=" + record.offset());
                            TopicPartition topicPartition = new TopicPartition(deplyTopic, record.partition());
                            consumer.seek(topicPartition, record.offset());
                            continue;
                        }
                        nextTopic = KafkaDelayManager.getDelayTopicName(metaTime.getName());
                    } else {
                        nextTopic = targetTopic;
                        // TODO, 测试用
                        KafkaDelayManager.removeDelayTopicCount(value);
                    }
                    dispatchMessage(originTopic, nextTopic, record.value(), targetTopic, startDeliverTimeByteArray, retryNumsBytes);
                    // Map<分区，偏移量>
                    Map<TopicPartition, OffsetAndMetadata> partitionOffSetMap = new HashMap<>();
                    partitionOffSetMap.put(new TopicPartition(deplyTopic, record.partition()), new OffsetAndMetadata(record.offset() + 1));
                    consumer.commitSync(partitionOffSetMap);
//                    System.err.println("commitCurrTime:" + System.currentTimeMillis() + ",提交偏移量,topic:" + record.topic() + ",offset=" + (record.offset() + 1));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }



    private void dispatchMessage(String originTopic, String nextTopic, String value, String targetTopic, byte[] startDeliverTimeByteArray, byte[] retryNumsBytes)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        ProducerRecord<String, String> nextRecord = new ProducerRecord<>(nextTopic, value);
        // 源topic
        nextRecord.headers().add(KafkaConstants.DelayConstants.ORIGIN_TOPIC, originTopic.getBytes());
        // 目标topic
        nextRecord.headers().add(KafkaConstants.DelayConstants.TARGET_TOPIC, targetTopic.getBytes());
        // 开始投递时间
        nextRecord.headers().add(KafkaConstants.DelayConstants.START_DELIVER_TIME, startDeliverTimeByteArray);
        // 消费重试次数
        nextRecord.headers().add(KafkaConstants.RetryConstants.RETRY_TIMES, retryNumsBytes);
        KafkaDelayManager.dispatchMessage(nextRecord).get();
    }

    private Map<String, Object> getRecordHeaderMap(ConsumerRecord<String, String> record){
        Header[] headers = record.headers().toArray();
        Map<String, Object> resultMap = new HashMap<>(headers.length);
        for (Header header : headers) {
            String key = header.key();
            if (key.equals(KafkaConstants.DelayConstants.TARGET_TOPIC)) {
                resultMap.put(KafkaConstants.DelayConstants.TARGET_TOPIC, new String(header.value()));
            } else if(key.equals(KafkaConstants.DelayConstants.START_DELIVER_TIME)){
                resultMap.put(KafkaConstants.DelayConstants.START_DELIVER_TIME, header.value());
            } else if(key.equals(KafkaConstants.RetryConstants.RETRY_TIMES)){
                resultMap.put(KafkaConstants.RetryConstants.RETRY_TIMES, header.value());
            } else if(key.equals(KafkaConstants.DelayConstants.ORIGIN_TOPIC)){
                resultMap.put(KafkaConstants.DelayConstants.ORIGIN_TOPIC, new String(header.value()));
            }
        }
        return resultMap;
    }

    public void stop(){
        isRunning = false;
        syncClose();
    }


    private void syncClose(){
        try {
            lock.lock();
            consumer.close();
        } finally {
            lock.unlock();
        }
    }


}