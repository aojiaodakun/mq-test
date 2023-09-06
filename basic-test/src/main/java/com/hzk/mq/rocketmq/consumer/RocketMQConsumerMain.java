package com.hzk.mq.rocketmq.consumer;

import com.hzk.mq.rocketmq.constants.RocketMQConstants;
import com.hzk.mq.rocketmq.util.RocketMQAdminExtUtil;
import com.hzk.mq.rocketmq.util.RocketMQTopicUtil;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.PullMessageService;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Set;

public class RocketMQConsumerMain {

    static {
        System.setProperty(RocketMQConstants.REGION_CONSUMER_ENABLE, "true");
//        System.setProperty(RocketMQConstants.REGION_CONSUMER_ENABLE, "false");
    }

    public static void main(String[] args) throws Exception{




        String groupName = "hzkConsumerGroup_1";
        String topic = "test0808_01";
//        RocketMQTopicUtil.createTopic(topic, 2);

        RPCHook rpcHook = new RPCHook() {
            RPCHook authRpcHook = new AclClientRPCHook(new SessionCredentials("rocketmq2", "12345678"));

            @Override
            public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
                authRpcHook.doBeforeRequest(remoteAddr, request);
                if (request.getCode() == 36) {
                    System.out.println("doBeforeRequest");
                }
            }

            @Override
            public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
                authRpcHook.doAfterResponse(remoteAddr, request, response);
                if (request.getCode() == 36) {// org.apache.rocketmq.common.protocol.RequestCode.CONSUMER_SEND_MSG_BACK
                    String group = request.getExtFields().get("group");
                    String originTopic = request.getExtFields().get("originTopic");
                    String originMsgId = request.getExtFields().get("originMsgId");
                    System.out.println("doAfterResponse");
                }
            }
        };

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(null, groupName, rpcHook);
//        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("hzk_consumer_group_name_07_25");
        consumer.setNamesrvAddr("localhost:9876");
        if (Boolean.getBoolean(RocketMQConstants.REGION_CONSUMER_ENABLE)) {
            consumer.setInstanceName("hzk");
        }
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 消费批次
        consumer.setConsumeMessageBatchMaxSize(1);

        MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(consumer, rpcHook);

//        String topic = "test1";
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
                int reconsumeTimes = messageExt.getReconsumeTimes();
                System.out.println("queue:" + messageExt.getQueueId() +
                        ",offset:" + messageExt.getQueueOffset() +
                        ",reconsumeTimes:" + reconsumeTimes
                        +",body:" + body);
                System.err.println("-------------------------------------");

//                if (body.contains("3")) {
//                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//                }
                // ConsumeConcurrentlyStatus.RECONSUME_LATER

                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });

//        RocketMQTopicUtil.createSubscriptionGroup(groupName);

        consumer.start();


//        MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(consumer, rpcHook);
//        PullMessageService pullMessageService = mqClientInstance.getPullMessageService();

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


        System.in.read();
    }



}
