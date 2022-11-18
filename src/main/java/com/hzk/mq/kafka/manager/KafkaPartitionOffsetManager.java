package com.hzk.mq.kafka.manager;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class KafkaPartitionOffsetManager {
    // Map<topic_group,offset>
    private static Map<String, ArrayBlockingQueue<TopicOffsetMetadata>> TOPICGROUP_OFFSET_MAP = new ConcurrentHashMap<>();

    public static void registQueue(String topic, String groupName){
        String key = getKey(topic, groupName);
        int capacity = Integer.parseInt(System.getProperty("kafka.consumer.offset.queue.capacity", "1000"));
        TOPICGROUP_OFFSET_MAP.put(key, new ArrayBlockingQueue<>(capacity));
    }

    public static boolean offer(String topic, String groupName, Map<TopicPartition, OffsetAndMetadata> partitionOffsetMap){
        String key = getKey(topic, groupName);
        ArrayBlockingQueue<TopicOffsetMetadata> topicOffsetMetadataQueue = TOPICGROUP_OFFSET_MAP.get(key);
        return topicOffsetMetadataQueue.offer(new TopicOffsetMetadata(topic, groupName, partitionOffsetMap));
    }

//    public static TopicOffsetMetadata poll(String topic, String groupName){
//        String key = getKey(topic, groupName);
//        ArrayBlockingQueue<TopicOffsetMetadata> topicOffsetMetadataQueue = TOPICGROUP_OFFSET_MAP.get(key);
//        if (topicOffsetMetadataQueue != null) {
//            return topicOffsetMetadataQueue.poll();
//        }
//        return null;
//    }

    public static TopicOffsetMetadata poll(String topic, String groupName, long timeout, TimeUnit unit){
        String key = getKey(topic, groupName);
        ArrayBlockingQueue<TopicOffsetMetadata> topicOffsetMetadataQueue = TOPICGROUP_OFFSET_MAP.get(key);
        if (topicOffsetMetadataQueue != null) {
            try {
                return topicOffsetMetadataQueue.poll(timeout, unit);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private static String getKey(String topic, String groupName){
        return topic + "_" + groupName;
    }

    public static class TopicOffsetMetadata {
        String topic;
        String groupName;
        Map<TopicPartition, OffsetAndMetadata> partitionOffsetMap;

        public TopicOffsetMetadata(String topic, String groupName, Map<TopicPartition, OffsetAndMetadata> partitionOffsetMap) {
            this.topic = topic;
            this.groupName = groupName;
            this.partitionOffsetMap = partitionOffsetMap;
        }

        public String getTopic() {
            return topic;
        }

        public String getGroupName() {
            return groupName;
        }

        public Map<TopicPartition, OffsetAndMetadata> getPartitionOffsetMap() {
            return partitionOffsetMap;
        }
    }

}
