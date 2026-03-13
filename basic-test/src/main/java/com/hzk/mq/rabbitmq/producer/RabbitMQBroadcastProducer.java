package com.hzk.mq.rabbitmq.producer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.AMQP;
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

//            int max = 1024 * 2;
            int max = 10;
            for (int i = 0; i < max; i++) {
//                String message = "Hello World，靓仔" + i;
//                byte[] bytes = message.getBytes();

                byte[] bytes = new byte[1024 * 1024];// 1M
                //消息的routingKey就是队列名称

                // 持久化
//                channel.basicPublish(EXCHANGE_NAME, "", null, bytes);

                // 非持久化
                channel.basicPublish(EXCHANGE_NAME, "", new AMQP.BasicProperties.Builder().deliveryMode(1).build(), bytes);

//                System.out.println(" [x] Sent '" + message + "'");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            channel.close();
            connection.close();
        }
    }

}
