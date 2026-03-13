package com.hzk.mq.rocketmq.producer;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 消息大小max默认4m
 */
public class RocketMQProducerTest {

    public static void main(String[] args) throws Exception{

        Logger logger = LoggerFactory.getLogger(RocketMQProducerTest.class);
        logger.info("test");

//        RPCHook rpcHook = new AclClientRPCHook(new SessionCredentials("rocketmq2", "12345678"));
//        DefaultMQProducer producer = new DefaultMQProducer("hzk_producer_group_name", rpcHook);
        DefaultMQProducer producer = new DefaultMQProducer("hzk_producer_groTopicTest5up_name");
        producer.setNamesrvAddr("localhost:9876");
        producer.setMaxMessageSize(1024 * 1024 * 10);// 10M，客户端设置超过4M无意义，得broker提高maxMessageSize
        producer.setRetryTimesWhenSendFailed(5);
        producer.setSendMsgTimeout(100000);
//        producer.setNamesrvAddr("localhost:9876;localhost:9877");
        producer.start();

        String topic = "TopicTest";

        for (int i = 0; i < 1; i++) {
            try {
                Message msg = new Message(topic/* Topic */,
                        "TagA" /* Tag */,
                        ("test09-04-hzk" + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
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
