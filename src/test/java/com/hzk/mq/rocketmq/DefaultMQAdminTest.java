package com.hzk.mq.rocketmq;

import com.hzk.mq.rocketmq.util.RocketMQAdminExtUtil;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

public class DefaultMQAdminTest {

    @Test
    public void test1() throws Exception{
        DefaultMQAdminExt mqAdminExt = RocketMQAdminExtUtil.getMQAdminExt();
        String clusterName = "DefaultCluster";
        String topic = "test-01-13";
        int queueNum = 4;

        // TODO,删除topic
//        mqAdminExt.deleteTopicInNameServer(Collections.singleton(namesrvAddr), topic);

        Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(mqAdminExt, clusterName);
        for (String address : masterSet) {
            TopicStatsTable topicStatsTable;
            try {
                topicStatsTable = mqAdminExt.examineTopicStats(topic);
                System.out.println(topicStatsTable);
            } catch (Exception ex) {

            }
            TopicRouteData topicRouteData;
            try {
                topicRouteData = mqAdminExt.examineTopicRouteInfo(topic);
                System.out.println(topicRouteData);
            } catch (Exception ex) {

            }
            TopicConfig topicConfig = mqAdminExt.examineTopicConfig(address, topic);
            if (topicConfig == null) {
                // 创建topic
                topicConfig = new TopicConfig(topic);
                topicConfig.setReadQueueNums(queueNum);
                topicConfig.setWriteQueueNums(queueNum);
                mqAdminExt.createAndUpdateTopicConfig(address, topicConfig);
            }
        }

        ClusterInfo clusterInfo = mqAdminExt.examineBrokerClusterInfo();
        Map<String, BrokerData> brokerAddrMap = clusterInfo.getBrokerAddrTable();

//        mqAdminExt.createAndUpdateTopicConfig();

        mqAdminExt.shutdown();
    }


}
