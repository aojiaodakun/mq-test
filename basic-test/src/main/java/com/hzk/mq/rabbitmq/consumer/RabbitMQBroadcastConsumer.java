package com.hzk.mq.rabbitmq.consumer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;

/**
 * rabbitmq纯广播模式，消费者
 */
public class RabbitMQBroadcastConsumer {


    protected final static String EXCHANGE_NAME = "exchange_fanout_hzk";

    public static void main(String[] args) throws Exception {
        Connection connection = null;
        Channel channel = null;
        try {
            connection = RabbitMQFactory.getConnection();
            channel = connection.createChannel();

            //声明exchange,类型为fanout,消息路由到所有绑定的队列，广播模式
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, EXCHANGE_NAME, "");

            channel.basicConsume(queueName, true, new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    System.out.println("接收数据:" + new String(body));
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}

