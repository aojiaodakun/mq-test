package com.hzk.mq.rocketmq;

import com.hzk.mq.rocketmq.constants.RocketMQConstants;
import com.hzk.mq.rocketmq.util.RocketMQAdminExtUtil;
import com.hzk.mq.rocketmq.util.RocketMQTopicUtil;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.Collections;
import java.util.Set;

public class RocketMQAdminTest {



    public static void main(String[] args) throws Exception{

        DefaultMQAdminExt mqAdminExt = RocketMQAdminExtUtil.getMQAdminExt();


        String groupName = "hzkConsumerGroup_1";
        String clusterName = "DefaultCluster";
        Set<String> masterSet = RocketMQTopicUtil.getMasterAddrSet(mqAdminExt, clusterName);
        for (String masterIp : masterSet) {
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
