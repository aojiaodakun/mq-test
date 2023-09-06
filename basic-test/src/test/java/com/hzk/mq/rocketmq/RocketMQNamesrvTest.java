package com.hzk.mq.rocketmq;

import com.hzk.mq.rocketmq.constants.RocketMQConstants;
import com.hzk.mq.rocketmq.util.RocketMQAdminExtUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Test;

public class RocketMQNamesrvTest {

    static {
        System.setProperty(RocketMQConstants.MQ_ROCKETMQ_NAMESRVADDR_DEFAULT, "localhost:9876;localhost:9877");
    }

    @Test
    public void testNamesrv() throws Exception {
        DefaultMQAdminExt mqAdminExt = RocketMQAdminExtUtil.getMQAdminExt();
        mqAdminExt.createAndUpdateKvConfig("namespace1", "k1" , "v11");
        mqAdminExt.getKVConfig("namespace1", "k1");

    }


}
