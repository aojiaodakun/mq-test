package com.hzk.mq.kafka.consumer;

import com.hzk.mq.kafka.constant.KafkaConstants;
import com.hzk.mq.kafka.util.KafkaAdminUtil;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaRealLogConsumer {

    public static void main(String[] args) {
        // 本地
        System.setProperty(KafkaConstants.BOOTSTRAP_SERVERS, "localhost:9092");

        String topic = "TopicTest";

        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka.eagle.system.group");
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        List<String> topicList = new ArrayList<>();
        topicList.add("TopicTest");
        AdminClient adminClient = KafkaAdminUtil.getAdminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicList);
        describeTopicsResult.values().forEach((k,v)->{
            System.out.println(k);
            try {
                TopicDescription topicDescription = v.get();
                List<TopicPartitionInfo> topicPartitionInfoList = topicDescription.partitions();
                for(TopicPartitionInfo tempPartitionInfo : topicPartitionInfoList) {
                    int partition = tempPartitionInfo.partition();
                    TopicPartition tp = new TopicPartition(topic, partition);
                    consumer.assign(Collections.singleton(tp));
                    java.util.Map<TopicPartition, Long> endLogSize = consumer.endOffsets(Collections.singleton(tp));
                    java.util.Map<TopicPartition, Long> startLogSize = consumer.beginningOffsets(Collections.singleton(tp));
                    try {
                        long realLogSize = endLogSize.get(tp).longValue() - startLogSize.get(tp).longValue();
                        System.out.println("partition=" + partition + ",logSize=" + realLogSize);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        });



        if (consumer != null) {
            consumer.close();
        }

    }

}
