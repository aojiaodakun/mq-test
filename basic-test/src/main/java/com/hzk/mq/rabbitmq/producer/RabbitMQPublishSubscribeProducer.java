package com.hzk.mq.rabbitmq.producer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * Publish/Subscribe(发布/订阅模式),一条消息被发送到多个队列
 */
public class RabbitMQPublishSubscribeProducer {

    private static final String EXCHANGE_NAME = "logs";
    private final static String QUEUE_NAME_1 = "destination_terminal";
    private final static String QUEUE_NAME_2 = "destination_file";

    public static void main(String[] args) throws Exception{
        Connection connection = null;
        Channel channel = null;
        try {
            connection = RabbitMQFactory.getConnection();
            channel = connection.createChannel();
            //声明exchange,类型为fanout,消息路由到所有绑定的队列，广播模式
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
            //绑定
            channel.queueBind(QUEUE_NAME_1, EXCHANGE_NAME, "");
//            channel.queueBind(QUEUE_NAME_2, EXCHANGE_NAME, "");
            for (int i = 0; i < 20; i++) {
                String message = "消息-" + i;
                channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
                System.out.println(" [x] Sent '" + message + "'");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            channel.close();
            connection.close();
        }
    }


}
