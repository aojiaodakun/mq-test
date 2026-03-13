package com.hzk.boot;

import com.hzk.constants.RocketMQConstants;
import com.hzk.util.RocketMQAdminExtUtil;
import com.hzk.util.RocketMQTopicUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import javax.crypto.Mac;
import java.security.Security;
import java.util.Map;
import java.util.Set;

/**
 * host=127.0.0.1:9876
 * topic=test_queue
 * user=rabbitmq-client
 * password=rabbitmq-client
 * clusterName=DefaultCluster
 * 全部参数
 * host=127.0.0.1:9876 topic=test_queue clusterName=DefaultCluster
 * host=127.0.0.1:9876 topic=test_queue clusterName=DefaultCluster user=user password=password
 */
public class RocketMQApplication {


    public static void main(String[] args) throws Exception{
        for (int i = 0; i < args.length; i++) {
            String[] tempArr = args[i].split("=");
            System.setProperty(tempArr[0], tempArr[1]);
        }
        checkAlgorithm();
        producerTest();
    }


    private static void checkAlgorithm() {
        try {
            Mac mac = Mac.getInstance("HmacSHA1");
            System.out.println("HmacSHA1 algorithm is available");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("HmacSHA1 algorithm is NOT available: " + e.getMessage());
        }
    }

    private static void producerTest() throws Exception {
        if (Boolean.getBoolean("rocketmq.security.addprovider.enable")) {
            try {
                Security.addProvider(new com.sun.crypto.provider.SunJCE());
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Failed to add security provider" + e.getMessage());
            }
        }
        String host = System.getProperty("host");
        System.setProperty(RocketMQConstants.MQ_ROCKETMQ_NAMESRVADDR_DEFAULT, host);
        // 获取集群名
        String topic = System.getProperty("topic");
        // 获取全部集群名
        Map<String, Set<String>> allClusterMap = RocketMQTopicUtil.getAllCluster(RocketMQAdminExtUtil.getMQAdminExt());
        System.out.println("getAllCluster result:" + allClusterMap);
        // 创建topic
        boolean flag = RocketMQTopicUtil.createTopic(topic, 2);
        System.err.println("createTopic " + flag + ",topic=" + topic);

        // 创建生产者，发送消息测试
        DefaultMQProducer producer = new DefaultMQProducer("hzk_producer_group", getAclRPCHook());
        producer.setNamesrvAddr(host);
        producer.setMaxMessageSize(1024 * 1024 * 1);
        producer.setRetryTimesWhenSendFailed(5);
        producer.setSendMsgTimeout(1000 * 3);
        producer.start();
        System.err.println("producer start success");
        try {
            Message msg = new Message(topic/* Topic */,
                    "TagA" /* Tag */,
                    ("test-body").getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.err.println("main end");
    }

    public static RPCHook getAclRPCHook() {
        String accessKey = System.getProperty("user");
        String secretKey = System.getProperty("password");
        return !StringUtils.isEmpty(accessKey) && !StringUtils.isEmpty(secretKey) ?
                new AclClientRPCHook(new SessionCredentials(accessKey, secretKey)) : null;
    }


}
