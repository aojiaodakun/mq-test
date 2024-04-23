package com.hzk.mq.rocketmq.producer;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 消息大小max默认4m
 */
public class RocketMQProducerTest {

    public static void main(String[] args) throws Exception{

        RPCHook rpcHook = new AclClientRPCHook(new SessionCredentials("rocketmq2", "12345678"));

        DefaultMQProducer producer = new DefaultMQProducer("hzk_producer_group_name", rpcHook);
        // 分号分割
        producer.setNamesrvAddr("localhost:9876");
        producer.setRetryTimesWhenSendFailed(5);
        producer.setSendMsgTimeout(10000);
//        producer.setNamesrvAddr("localhost:9876;localhost:9877");
        producer.start();

        String topic = "TopicTest";
        for (int i = 0; i < 1; i++) {
            try {
                Message msg = new Message(topic/* Topic */,
                        "TagA" /* Tag */,
                        ("test08-08-hzk" + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );
                // 延迟等级
//                msg.setDelayTimeLevel(2);
                SendResult sendResult = producer.send(msg);
                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        producer.shutdown();
        System.err.println("-------------------------------------");
        // TODO sleep
        Thread.currentThread().sleep(100000);

    }


}
