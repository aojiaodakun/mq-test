package com.hzk.mq.rocketmq;

import com.hzk.mq.rocketmq.util.RocketMQTopicUtil;
import org.junit.Assert;
import org.junit.Test;

public class RocketMQSubscriptionGroupTest {

    @Test
    public void createSubscriptionGroup() throws Exception {
        String groupName = "hzkConsumerGroup4";
        boolean isSuccess = RocketMQTopicUtil.createSubscriptionGroup(groupName);
        Assert.assertEquals(isSuccess, Boolean.TRUE);
    }


    @Test
    public void deleteSubscriptionGroup() throws Exception {
        String groupName = "hzkConsumerGroup5";
        boolean isSuccess = RocketMQTopicUtil.deleteSubscriptionGroup(groupName);
        Assert.assertEquals(isSuccess, Boolean.TRUE);
    }

}
