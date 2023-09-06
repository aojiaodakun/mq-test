package com.hzk.mq.rocketmq.util;

import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.MQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * 若开启了自动创建TOPIC(autoCreateTopicEnable=true),默认创建一个TOPIC,TOPIC绑定读写各四条队列(可在客户端手动更改).
 * 注意：只有生产者往对应的TOPIC发消息时才会自动创建。官网建议线下开启，线上关闭自动创建，
 * 因为可能第一次发的消息数量不够队列数量，只会在一个节点上创建TOPIC达不到负载均衡。
 * 所以在以上背景下，通过此API在初始化Consumer的时候就在各个Broker上把Topic创建好
 */
public class RocketMQTopicUtil {

    private static Map<String, Set<String>> CLUSTERNAME_MASTERADDR_MAP = new ConcurrentHashMap<>(2);

    /**
     * 在指定nameServer的clusterName下使用默认参数创建topic
     *
     * @param topic topic
     * @param queueNums 读写队列数
     * @return 布尔值
     */
    public static boolean createTopic(String topic, int queueNums) {
        DefaultMQAdminExt mqAdminExt = RocketMQAdminExtUtil.getMQAdminExt();
        try {
            String clusterName = "DefaultCluster";


//            mqAdminExt.createTopic(clusterName, topic, queueNums);

            Set<String> masterSet = getMasterAddrSet(mqAdminExt, clusterName);
            for (String address : masterSet) {
                // 判断topic是否存在
                TopicConfig topicConfig = mqAdminExt.examineTopicConfig(address, topic);
                if (topicConfig == null) {
                    topicConfig = new TopicConfig(topic);
                    topicConfig.setReadQueueNums(queueNums);
                    topicConfig.setWriteQueueNums(queueNums);
                    mqAdminExt.createAndUpdateTopicConfig(address, topicConfig);
                }
            }
            return true;
        } catch (Exception e) {
            System.err.println("error when RocketMQTopicUtil createTopic");
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 在每个masterIP下创建订阅组
     *
     * @param groupName groupName
     * @return 布尔值
     */
    public static boolean createSubscriptionGroup(String groupName) {
        try {
            DefaultMQAdminExt mqAdminExt = RocketMQAdminExtUtil.getMQAdminExt();
            String clusterName = "DefaultCluster";
            Set<String> masterSet = getMasterAddrSet(mqAdminExt, clusterName);
            for (String address : masterSet) {
                // 创建订阅组
                SubscriptionGroupConfig subscriptionGroupConfig = mqAdminExt.examineSubscriptionGroupConfig(address, groupName);
                if (subscriptionGroupConfig == null) {
                    subscriptionGroupConfig = new SubscriptionGroupConfig();
                    subscriptionGroupConfig.setGroupName(groupName);
                    mqAdminExt.createAndUpdateSubscriptionGroupConfig(address, subscriptionGroupConfig);
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }


    public static Set<String> getMasterAddrSet(MQAdminExt mqAdminExt, String clusterName) throws Exception{
        if (CLUSTERNAME_MASTERADDR_MAP.containsKey(clusterName)) {
            return CLUSTERNAME_MASTERADDR_MAP.get(clusterName);
        }
        Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(mqAdminExt, clusterName);
        CLUSTERNAME_MASTERADDR_MAP.putIfAbsent(clusterName, masterSet);
        return masterSet;
    }

}
