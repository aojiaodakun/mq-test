package com.hzk.mq.rocketmq.consumer;

import com.hzk.mq.rocketmq.constants.RocketMQConstants;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class RocketMQPullConsumerMain {


    static {
        System.setProperty(RocketMQConstants.REGION_CONSUMER_ENABLE, "true");
//        System.setProperty(RocketMQConstants.REGION_CONSUMER_ENABLE, "false");
    }

    public static void main(String[] args) throws Exception{
        String groupName = "hzkConsumerGroup_1";
        String topic = "TopicTest";

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(null, groupName);
//        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("hzk_consumer_group_name_07_25");
        consumer.setNamesrvAddr("localhost:9876");
        if (Boolean.getBoolean(RocketMQConstants.REGION_CONSUMER_ENABLE)) {
            consumer.setInstanceName("hzk");
        }
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 消费批次
        consumer.setConsumeMessageBatchMaxSize(1);


//        String topic = "test1";
        consumer.subscribe(topic, "*");
        consumer.setConsumeThreadMin(5);
        consumer.setConsumeThreadMax(50);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
//                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                MessageExt messageExt = msgs.get(0);
                String body = "";
                try {
                    body = new String(messageExt.getBody(), "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                int reconsumeTimes = messageExt.getReconsumeTimes();
                System.out.println("queue:" + messageExt.getQueueId() +
                        ",offset:" + messageExt.getQueueOffset() +
                        ",reconsumeTimes:" + reconsumeTimes
                        +",body:" + body);
                System.err.println("-------------------------------------");


                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });


        consumer.start();


        System.out.printf("Consumer Started.%n");




        System.in.read();
    }



}
