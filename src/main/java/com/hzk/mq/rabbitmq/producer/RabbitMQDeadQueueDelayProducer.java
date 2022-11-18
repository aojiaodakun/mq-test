package com.hzk.mq.rabbitmq.producer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.hzk.mq.support.delay.MetaTime;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 基于死信队列
 * 实现延迟功能，生产者
 */
public class RabbitMQDeadQueueDelayProducer {

    // 普通交换机
    private final static String NORMAL_EXCHANGE = "normal_exchange";

    public static void main(String[] args) throws Exception{
        Connection connection = null;
        Channel channel = null;
        try {
            connection = RabbitMQFactory.getConnection();
            channel = connection.createChannel();

            String routingKey = "normal";
            AMQP.BasicProperties properties = new AMQP.BasicProperties().builder()
                    .expiration("10000").build();
            for (int i = 0; i < 1; i++) {
                String message = "10000_info" + i;
                channel.basicPublish(NORMAL_EXCHANGE, routingKey, properties, message.getBytes());
                System.out.println(" [x] Sent '" + routingKey + ":" + message + "'");
            }


            AMQP.BasicProperties properties1 = new AMQP.BasicProperties().builder()
                    .expiration("2000").build();
            for (int i = 0; i < 1; i++) {
                String message = "2000_info" + i;
                channel.basicPublish(NORMAL_EXCHANGE, routingKey, properties1, message.getBytes());
                System.out.println(" [x] Sent '" + routingKey + ":" + message + "'");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            channel.close();
            connection.close();
        }
    }

}
