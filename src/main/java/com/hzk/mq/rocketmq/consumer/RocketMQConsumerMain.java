package com.hzk.mq.rocketmq.consumer;

import com.hzk.mq.rocketmq.constants.RocketMQConstants;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class RocketMQConsumerMain {

    static {
        System.setProperty(RocketMQConstants.REGION_CONSUMER_ENABLE, "true");
    }

    public static void main(String[] args) throws Exception{
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("hzk_consumer_group_name");
        consumer.setNamesrvAddr("localhost:9876");
        if (Boolean.parseBoolean(RocketMQConstants.REGION_CONSUMER_ENABLE)) {
            consumer.setInstanceName("hzk");
        }
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
//        String topic = "TopicTest";
        String topic = "test-01-13";
        consumer.subscribe(topic, "*");
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
                System.out.println("queue:" + messageExt.getQueueId() + ",offset:" + messageExt.getQueueOffset() + ",body=" + body);
                System.err.println("-------------------------------------");

                if (body.contains("3")) {
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");



//        DefaultMQPushConsumer consumer2 = new DefaultMQPushConsumer("hzk_consumer_group_name2");
//        consumer2.setNamesrvAddr("localhost:9876");
//        consumer2.setInstanceName("hzk");
//        consumer2.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
//        consumer2.subscribe("TopicTest2", "*");
//        consumer2.registerMessageListener(new MessageListenerConcurrently() {
//            @Override
//            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
//                                                            ConsumeConcurrentlyContext context) {
//                System.out.printf("%s Receive New Messages2222: %s %n", Thread.currentThread().getName(), msgs);
//                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//            }
//        });
//        consumer2.start();
//        System.out.printf("Consumer2222 Started.%n");
//

        System.in.read();
    }



}
