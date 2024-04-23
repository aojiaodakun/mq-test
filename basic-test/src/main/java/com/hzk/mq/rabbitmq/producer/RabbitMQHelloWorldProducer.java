package com.hzk.mq.rabbitmq.producer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * HelloWorld，一个生产者，一个消费者
 */
public class RabbitMQHelloWorldProducer {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws Exception {
        Connection connection = null;
        Channel channel = null;
        try {
            connection = RabbitMQFactory.getConnection();
            channel = connection.createChannel();

            for (int i = 0; i < 10; i++) {
                String message = "Hello World，hzk_" + i;
                //消息的routingKey就是队列名称
                channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
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
