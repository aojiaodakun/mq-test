package com.hzk.mq.rocketmq;

import com.hzk.mq.rocketmq.util.RocketMQAdminExtUtil;
import com.hzk.mq.rocketmq.util.RocketMQTopicUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.topic.DeleteTopicSubCommand;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
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
        DefaultMQAdminExt mqAdminExt = RocketMQAdminExtUtil.getMQAdminExt();
        DeleteTopicSubCommand.deleteTopic(mqAdminExt, clusterName, topic);
    }

}
