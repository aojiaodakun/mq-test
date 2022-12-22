package com.hzk.mq.rocketmq;

import com.hzk.mq.rocketmq.util.RocketMQTopicUtil;
import org.junit.Assert;
import org.junit.Test;

public class RocketMQTopicTest {

    @Test
    public void createTopicTest() throws Exception {
        boolean isSuccess = RocketMQTopicUtil.createTopic("localhost:9876", "test12-22-01", 6, null);
        Assert.assertEquals(isSuccess, Boolean.TRUE);
    }


}
