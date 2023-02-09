package com.hzk.mq.rocketmq.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 消息大小max默认4m
 */
public class RocketMQProducerTest {

    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("hzk_producer_group_name");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        String topic = "test1";
        for (int i = 0; i < 5; i++) {
            try {
                Message msg = new Message(topic/* Topic */,
                        "TagA" /* Tag */,
                        ("01-10-" + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
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
