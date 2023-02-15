package com.hzk.mq.rocketmq;

import com.hzk.mq.rocketmq.util.RocketMQAdminExtUtil;
import com.hzk.mq.rocketmq.util.RocketMQTopicUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.topic.DeleteTopicSubCommand;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class RocketMQTopicTest {

    @Test
    public void createTopicTest() throws Exception {
        boolean isSuccess = RocketMQTopicUtil.createTopic( "test12", 4);
        Assert.assertEquals(isSuccess, Boolean.TRUE);
    }


    @Test
    public void deleteTopicTest() throws Exception {
        String topic = "test12";
        String clusterName = "DefaultCluster";
        DefaultMQAdminExt adminExt = RocketMQAdminExtUtil.getMQAdminExt();

        /**
         * 不可用，不能调slave的
         * fetchMasterAndSlaveAddrByClusterName
         * MQClientException: CODE: 1  DESC: Can't modify topic or subscription group from slave broker, please execute it from master broker.
         */
//        DeleteTopicSubCommand.deleteTopic(adminExt, clusterName, topic);

        Set<String> brokerAddressSet = CommandUtil.fetchMasterAddrByClusterName(adminExt, clusterName);
        adminExt.deleteTopicInBroker(brokerAddressSet, topic);

        Set<String> nameServerSet = null;
        if (adminExt.getNamesrvAddr() != null) {
            String[] ns = adminExt.getNamesrvAddr().trim().split(";");
            nameServerSet = new HashSet(Arrays.asList(ns));
        }
        adminExt.deleteTopicInNameServer(nameServerSet, topic);

    }

}
