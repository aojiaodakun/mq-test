package com.hzk.mq.rocketmq;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.hzk.mq.rocketmq.constants.RocketMQConstants;
import com.hzk.mq.rocketmq.util.RocketMQAdminExtUtil;
import com.hzk.mq.rocketmq.util.RocketMQTopicUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class RocketMQAdminTest {



    public static void main(String[] args) throws Exception{

        DefaultMQAdminExt mqAdminExt = RocketMQAdminExtUtil.getMQAdminExt();
        String topic = "TopicTest";

        GroupList groupList = mqAdminExt.queryTopicConsumeByWho(topic);
        for (String groupName : groupList.getGroupList()) {
            ConsumeStats consumeStats = null;
            try {
                consumeStats = mqAdminExt.examineConsumeStats(groupName, topic);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            List<MessageQueue> mqList = Lists.newArrayList(Iterables.filter(consumeStats.getOffsetTable().keySet(), new Predicate<MessageQueue>() {
                @Override
                public boolean apply(MessageQueue o) {
                    return StringUtils.isBlank(topic) || o.getTopic().equals(topic);
                }
            }));
            Collections.sort(mqList);
            System.out.println(mqList);
            Map<MessageQueue, String> results = Maps.newHashMap();
            try {
                ConsumerConnection consumerConnection = mqAdminExt.examineConsumerConnectionInfo(groupName);
                for (Connection connection : consumerConnection.getConnectionSet()) {
                    String clinetId = connection.getClientId();
                    ConsumerRunningInfo consumerRunningInfo = mqAdminExt.getConsumerRunningInfo(groupName, clinetId, false);
                    for (MessageQueue messageQueue : consumerRunningInfo.getMqTable().keySet()) {
                        results.put(messageQueue, clinetId);
                    }
                }
            }
            catch (Exception err) {
                err.printStackTrace();
            }


//            List<TopicConsumerInfo> topicConsumerInfoList = Lists.newArrayList();
//            TopicConsumerInfo nowTopicConsumerInfo = null;
//            Map<MessageQueue, String> messageQueueClientMap = getClientConnection(groupName);
//            for (MessageQueue mq : mqList) {
//                if (nowTopicConsumerInfo == null || (!StringUtils.equals(mq.getTopic(), nowTopicConsumerInfo.getTopic()))) {
//                    nowTopicConsumerInfo = new TopicConsumerInfo(mq.getTopic());
//                    topicConsumerInfoList.add(nowTopicConsumerInfo);
//                }
//                QueueStatInfo queueStatInfo = QueueStatInfo.fromOffsetTableEntry(mq, consumeStats.getOffsetTable().get(mq));
//                queueStatInfo.setClientInfo(messageQueueClientMap.get(mq));
//                nowTopicConsumerInfo.appendQueueStatInfo(queueStatInfo);
//            }
//            return topicConsumerInfoList;
        }

        String groupName = "hzkConsumerGroup_1";
        String clusterName = "DefaultCluster";
        Set<String> masterSet = RocketMQTopicUtil.getMasterAddrSet(mqAdminExt, clusterName);
        for (String masterIp : masterSet) {

            TopicList topicList = mqAdminExt.fetchTopicsByCLuster("DefaultCluster");
            TopicConfig topicConfig = mqAdminExt.examineTopicConfig(masterIp, topic);
            TopicRouteData topicRouteData = mqAdminExt.examineTopicRouteInfo(topic);
            List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
            BrokerData brokerData = brokerDataList.get(0);
            String brokerName = brokerData.getBrokerName();
            long minOffset0 = mqAdminExt.minOffset(new MessageQueue(topic, brokerName, 0));
            long minOffset1 = mqAdminExt.minOffset(new MessageQueue(topic, brokerName, 1));
            long minOffset2 = mqAdminExt.minOffset(new MessageQueue(topic, brokerName, 2));
            long minOffset3 = mqAdminExt.minOffset(new MessageQueue(topic, brokerName, 3));
            long maxOffset0 = mqAdminExt.maxOffset(new MessageQueue(topic, brokerName, 0));
            long maxOffset1 = mqAdminExt.maxOffset(new MessageQueue(topic, brokerName, 1));
            long maxOffset2 = mqAdminExt.maxOffset(new MessageQueue(topic, brokerName, 2));
            long maxOffset3 = mqAdminExt.maxOffset(new MessageQueue(topic, brokerName, 3));
            // TODO
            KVTable kvTable = mqAdminExt.fetchBrokerRuntimeStats(masterIp);

            TopicStatsTable topicStatsTable = mqAdminExt.examineTopicStats(topic);
            SubscriptionGroupWrapper userSubscriptionGroup = mqAdminExt.getUserSubscriptionGroup(masterIp, 5000);


            SubscriptionGroupWrapper allSubscriptionGroup = mqAdminExt.getUserSubscriptionGroup(masterIp, 5000);

            // 创建订阅组
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(groupName);
            mqAdminExt.createAndUpdateSubscriptionGroupConfig(masterIp, subscriptionGroupConfig);


//            mqAdminExt.deleteTopicInNameServer(namesrvSet, "tttt");
            // 删除订阅组
            mqAdminExt.deleteSubscriptionGroup(masterIp, "hzk_consumer_group_name1", true);



            allSubscriptionGroup = mqAdminExt.getUserSubscriptionGroup(masterIp, 5000);
            System.out.println(allSubscriptionGroup);
        }

    }

}
