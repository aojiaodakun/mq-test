package com.hzk.mq.rabbitmq.producer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * rabbitmq纯广播模式，生产者
 */
public class RabbitMQBroadcastProducer {

    protected final static String EXCHANGE_NAME = "exchange_fanout_hzk";

    public static void main(String[] args) throws Exception {
        Connection connection = null;
        Channel channel = null;
        try {
            connection = RabbitMQFactory.getConnection();
            channel = connection.createChannel();

            for (int i = 0; i < 10; i++) {
                String message = "Hello World，靓仔" + i;
                //消息的routingKey就是队列名称
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
