package com.hzk.mq.kafka.consumer;

import com.hzk.mq.kafka.KafkaAcker;
import com.hzk.mq.kafka.common.KafkaConsumerWorkerPool;
import com.hzk.mq.kafka.common.KafkaDispatchProducer;
import com.hzk.mq.kafka.config.KafkaConfig;
import com.hzk.mq.kafka.constant.KafkaConstants;
import com.hzk.mq.kafka.delay.KafkaDelayManager;
import com.hzk.mq.kafka.util.KafkaAdminUtil;
import com.hzk.mq.support.delay.MetaTime;
import com.hzk.mq.support.util.ClassCastUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 1、动态监听多个topic
 * 2、异步提交偏移量
 * 3、分开拉取线程，提交线程，异步工作线程，业务并发度=2
 * 4、消费重试（即包含延迟消息）
 */
public class AllFeatureConsumerMain {

    // Map<appId, topics>
    private static Map<String, Set<String>> APPID_TOPICLIST_MAP = new HashMap<>();

    private static Map<String, PollThread> APPID_POLLTHREAD_MAP = new HashMap<>();

    private static Map<String, CommitThread> APPID_COMMITTHREAD_MAP = new HashMap<>();

    static final String APPID_HR = "hr";
    static final String APPID_FI = "fi";

    static {
        Set<String> hrTopicSet = new HashSet<>();
        hrTopicSet.add("hr_topic1");
        hrTopicSet.add("hr_topic2");
        hrTopicSet.add("hr_topic3");
        APPID_TOPICLIST_MAP.put(AllFeatureConsumerMain.APPID_HR, hrTopicSet);

        Set<String> fiTopicSet = new HashSet<>();
        fiTopicSet.add("fi_topic1");
        fiTopicSet.add("fi_topic2");
        APPID_TOPICLIST_MAP.put(AllFeatureConsumerMain.APPID_FI, fiTopicSet);
    }

    public static void main(String[] args) {
        // 本地
        System.setProperty(KafkaConstants.BOOTSTRAP_SERVERS, "localhost:9092");
        // 最大消费重试次数
        System.setProperty(KafkaConstants.RetryConstants.MQ_KAFKA_CONSUMER_RETRY_TIMES, "3");
        /**
         * 1、启动延迟管理器
         * 2、每个appId使用一个消费者，监听appId下所有topic和重试topic，先启动hr和fi的消费者
         * 3、启动HttpServer，支持为appId动态添加监听topic。TODO
         * 4、分开PollThread，CommitThread。使用ReentrantLock保证同步调用sdk
         */
        // 1、启动延迟管理器
        KafkaDelayManager.start();

        // 2、每个appId使用一个消费者，监听appId下所有topic和重试topic，先启动hr和fi的消费者
        for(Map.Entry<String, Set<String>> entry:APPID_TOPICLIST_MAP.entrySet()){
            String appId = entry.getKey();
            // 创建重试topic
            String groupId = getGroupId(appId);
            String retryTopic = getRetryTopic(groupId);
            KafkaAdminUtil.createTopic(retryTopic, 4, (short) 1);
            // 创建常规topic
            Set<String> topicSet = entry.getValue();
            for(String tempTopic : topicSet) {
                KafkaAdminUtil.createTopic(tempTopic, 4, (short) 1);
            }
            Properties properties = KafkaConfig.getConsumerConfig();
            // 消费者组
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

            // 拉取线程
            String pollThreadName = "kafkaConsumer-poll-" + appId;
            ReentrantLock lock = new ReentrantLock();
            PollThread pollThread = new PollThread(pollThreadName, appId, consumer, lock);
            pollThread.start();
            APPID_POLLTHREAD_MAP.put(appId, pollThread);

            // 提交偏移量线程
            String commitThreadName = "kafkaConsumer-commit-" + appId;
            CommitThread commitThread = new CommitThread(commitThreadName, appId, consumer, lock, pollThread);
            commitThread.start();
            APPID_COMMITTHREAD_MAP.put(appId, commitThread);
        }
    }

    static String getRetryTopic(String groupId){
        return groupId + KafkaConstants.RetryConstants.RETRY_TOPIC_SUFFIX;
    }

    static boolean isRetryTopic(String topic){
        return topic.endsWith(KafkaConstants.RetryConstants.RETRY_TOPIC_SUFFIX);
    }

    private static String getGroupId(String appId){
        return appId + "_group";
    }

    static Set<String> getTopicSet(String appId) {
        return APPID_TOPICLIST_MAP.get(appId);
    }

    static int getConsumerMaxRetryTimes(){
        return Integer.getInteger(KafkaConstants.RetryConstants.MQ_KAFKA_CONSUMER_RETRY_TIMES);
    }



}
class PollThread extends Thread {

    private String appId;
    private KafkaConsumer<String, String> consumer;
    private ReentrantLock lock;
    private Set<String> topicSet;
    private int topicSize;
    private String groupId;
    private Map<String, Semaphore> appid2SemaphoreMap = new HashMap<>();

    private ArrayBlockingQueue<ConsumerRecord<String, String>> recordQueue = new ArrayBlockingQueue<>(1000);

    private ArrayBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsetQueue = new ArrayBlockingQueue<>(10000);

    public PollThread(String threadName, String appId, KafkaConsumer<String, String> consumer, ReentrantLock lock){
        setName(threadName);
        this.appId = appId;
        this.consumer = consumer;
        this.lock = lock;
        this.topicSet = AllFeatureConsumerMain.getTopicSet(appId);
        this.topicSize = topicSet.size();
        for(String tempTopic:topicSet){
            // 并发度=2
            appid2SemaphoreMap.put(tempTopic, new Semaphore(2));
        }
        groupId = consumer.groupMetadata().groupId();
    }

    @Override
    public void run() {
        syncSubscribe(topicSet);
        pollAndHandleMessage();
    }

    private void pollAndHandleMessage() {
        while (topicSize > 0) {
            int maxRetryTimes = AllFeatureConsumerMain.getConsumerMaxRetryTimes();
            // 优先处理内存队列,不考虑并发度
            while (recordQueue.size() > 0) {
                try {
                    handleMemoryQueueFirst(maxRetryTimes);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            // topics变更，重新订阅
            Set<String> cacheTopicSet = AllFeatureConsumerMain.getTopicSet(appId);
            if (topicSize != cacheTopicSet.size()) {
                topicSize = cacheTopicSet.size();
                if (handleTopicsChanged()) {
                    break;
                }
            }
            try {
                ConsumerRecords<String, String> consumerRecords = syncPoll();
                if (consumerRecords.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> record:consumerRecords) {
                    String topic = record.topic();
                    if (AllFeatureConsumerMain.isRetryTopic(topic)) {
                        topic = getHeaderOriginTopic(record);
                    }
                    Semaphore semaphore = appid2SemaphoreMap.get(topic);
                    try {
                        boolean flag = semaphore.tryAcquire(10, TimeUnit.MILLISECONDS);
                        if (!flag) {
                            // 加入内存队列，拉取下一批次前处理
                            boolean offerFlag = recordQueue.offer(record, 1000, TimeUnit.MILLISECONDS);
                            if (offerFlag) {
                                continue;
                            }
                            // 内存队列满，执行业务
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    // 使用线程池执行业务
                    KafkaConsumerWorkerPool.execute(() -> {
                        try {
                            KafkaAcker acker = new KafkaAcker();
                            int retryTimes = getHeaderRetryTimes(record);
                            handleDelivery(record, acker, retryTimes);
                            // 拒绝，发到延迟topic，消费重试
                            if (acker.isDenied()) {
                                retryTimes = retryTimes + 1;
                                /**
                                 * retryTimes<上限，转发至延迟topic
                                 * retryTimes>=上限，丢弃消息
                                 */
                                if (retryTimes < maxRetryTimes) {
                                    dispatchToDelayTopic(record, retryTimes);
                                } else {
                                    System.err.println("消费失败次数达上限.丢弃消息;topic=" + record.topic() + ",value=" + record.value());
                                }
                            }
                        } finally {
                            // 提交偏移量
                            commitOffset(record);
                            semaphore.release();
                        }
                    });
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void handleDelivery(ConsumerRecord<String, String> record, KafkaAcker acker, int retryTimes){
        String topic = record.topic();
        if (AllFeatureConsumerMain.isRetryTopic(topic)) {
            topic = getHeaderOriginTopic(record);
        }

        // TODO，业务逻辑

        // ack
//        acker.ack();
        // deny
        acker.deny();


    }

    private int getHeaderRetryTimes(ConsumerRecord<String, String> record){
        int retryTimes = 0;
        Map<String, Object> recordHeaderMap = getRecordHeaderMap(record);
        if (recordHeaderMap.containsKey(KafkaConstants.RetryConstants.RETRY_TIMES)) {
            retryTimes = ClassCastUtil.bytes2Int((byte[])recordHeaderMap.get(KafkaConstants.RetryConstants.RETRY_TIMES));
        }
        return retryTimes;
    }


    String getHeaderOriginTopic(ConsumerRecord<String, String> record){
        String originTopic = "";
        Map<String, Object> recordHeaderMap = getRecordHeaderMap(record);
        if (recordHeaderMap.containsKey(KafkaConstants.DelayConstants.ORIGIN_TOPIC)) {
            originTopic = recordHeaderMap.get(KafkaConstants.DelayConstants.ORIGIN_TOPIC).toString();
        }
        return originTopic;
    }

     Map<String, Object> getRecordHeaderMap(ConsumerRecord<String, String> record){
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

    private void handleMemoryQueueFirst(int maxRetryTimes) throws InterruptedException{
        ConsumerRecord<String, String> record = recordQueue.take();
        // 使用线程池执行业务
        KafkaConsumerWorkerPool.execute(() -> {
            KafkaAcker acker = new KafkaAcker();
            int retryTimes = getHeaderRetryTimes(record);
            try {
                handleDelivery(record, acker, retryTimes);
                // 拒绝，发到延迟topic，消费重试
                if (acker.isDenied()) {
                    retryTimes = retryTimes + 1;
                    /**
                     * retryTimes<上限，转发至延迟topic
                     * retryTimes>=上限，丢弃消息
                     */
                    if (retryTimes < maxRetryTimes) {
                        dispatchToDelayTopic(record, retryTimes);
                    }
                }
            } finally {
                // 提交偏移量
                commitOffset(record);
            }
        });
    }

    /**
     * 拒绝消息，转发至延迟topic
     * @param record record
     * @param retryTimes retryTimes
     */
    private void dispatchToDelayTopic(ConsumerRecord<String, String> record, int retryTimes) {
        MetaTime metaTime = MetaTime.genInstanceByLevel(retryTimes + 2);
        String delayTopic = KafkaDelayManager.getDelayTopicName(metaTime.getName());
        String targetTopic = AllFeatureConsumerMain.getRetryTopic(groupId);
        ProducerRecord<String, String> dispatchRecord = new ProducerRecord<>(delayTopic, record.value());
        // 源topic，第一次消费失败，源topic为空
        String originTopic = getHeaderOriginTopic(record);
        if (originTopic.equals("")) {
            originTopic = record.topic();
        }
        // 源topic
        dispatchRecord.headers().add(KafkaConstants.DelayConstants.ORIGIN_TOPIC, originTopic.getBytes());
        // 目标topic
        dispatchRecord.headers().add(KafkaConstants.DelayConstants.TARGET_TOPIC, targetTopic.getBytes());
        // 开始投递时间
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MILLISECOND, metaTime.getMillis());
        dispatchRecord.headers().add(KafkaConstants.DelayConstants.START_DELIVER_TIME, ClassCastUtil.longToBytes(calendar.getTime().getTime()));
        // 重试次数
        dispatchRecord.headers().add(KafkaConstants.RetryConstants.RETRY_TIMES, ClassCastUtil.int2bytes(retryTimes));
        KafkaDispatchProducer.send(dispatchRecord);
    }

    private boolean handleTopicsChanged() {
        if (topicSize == 0) {
            return true;
        }
        syncSubscribe(topicSet);
        for (String tempTopic:topicSet) {
            if (!appid2SemaphoreMap.containsKey(tempTopic)) {
                // 并发度=2
                Semaphore semaphore = new Semaphore(2);
                appid2SemaphoreMap.put(tempTopic, semaphore);
            }
        }
        return false;
    }

    private void syncSubscribe(Collection<String> topics) {
        try {
            lock.lock();
            topics.add(AllFeatureConsumerMain.getRetryTopic(this.groupId));
            this.consumer.subscribe(topics);
        } finally {
            lock.unlock();
        }
    }

    private ConsumerRecords<String, String> syncPoll(){
        try {
            lock.lock();
            return consumer.poll(Duration.ofMillis(100));
        } finally {
            lock.unlock();
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> pollOffset(){
        try {
            return offsetQueue.poll(10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 提交偏移量
     * @param record record
     */
    private void commitOffset(ConsumerRecord<String, String> record) {
        // Map<分区，偏移量>
        Map<TopicPartition, OffsetAndMetadata> partitionOffSetMap = new HashMap<>();
        partitionOffSetMap.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
        offsetQueue.offer(partitionOffSetMap);
    }

}

class CommitThread extends Thread {

    private String appId;
    private KafkaConsumer<String, String> consumer;
    private ReentrantLock lock;
    private Set<String> topicSet;
    private int topicSize;
    private PollThread pollThread;

    public CommitThread(String threadName, String appId, KafkaConsumer<String, String> consumer, ReentrantLock lock, PollThread pollThread){
        setName(threadName);
        this.appId = appId;
        this.consumer = consumer;
        this.lock = lock;
        this.topicSet = AllFeatureConsumerMain.getTopicSet(appId);
        this.topicSize = topicSet.size();
        this.pollThread = pollThread;
    }

    @Override
    public void run() {
        while (topicSize > 0) {
            Set<String> cacheTopicSet = AllFeatureConsumerMain.getTopicSet(appId);
            if (topicSize != cacheTopicSet.size()) {
                topicSize = cacheTopicSet.size();
                // 最后一次全部拉取，直到返回空代码队列为空
                if (topicSize == 0) {
                    while (true) {
                        Map<TopicPartition, OffsetAndMetadata> offsetMap = pollThread.pollOffset();
                        if (offsetMap == null) {
                            return;
                        } else {
                            syncSubmit(offsetMap);
                        }
                    }
                }
            }
            Map<TopicPartition, OffsetAndMetadata> offsetMap = pollThread.pollOffset();
            if (offsetMap != null) {
                syncSubmit(offsetMap);
            }
        }
    }

    private void syncSubmit(Map<TopicPartition, OffsetAndMetadata> offsetMap) {
        try {
            lock.lock();
            this.consumer.commitSync(offsetMap);
        } finally {
            lock.unlock();
        }
    }

}