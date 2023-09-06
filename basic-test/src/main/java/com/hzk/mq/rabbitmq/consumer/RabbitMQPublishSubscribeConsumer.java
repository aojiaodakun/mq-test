package com.hzk.mq.rabbitmq.consumer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;

/**
 * Publish/Subscribe(发布/订阅模式),一条消息被发送到多个队列
 */
public class RabbitMQPublishSubscribeConsumer {


    private final static String QUEUE_NAME_1 = "destination_terminal";
    private final static String QUEUE_NAME_2 = "destination_file";

    public static void main(String[] args) throws Exception {
        Connection connection = RabbitMQFactory.getConnection();
        Channel channel = connection.createChannel();
        //声明队列
        channel.queueDeclare(QUEUE_NAME_1, true, false, false, null);
        channel.queueDeclare(QUEUE_NAME_2, true, false, false, null);

        System.out.println("开始接受消息...");
        new Thread(() -> {
            try {
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println("destination_terminal Received '" + message + "'");
                };
                channel.basicConsume(QUEUE_NAME_1, true, deliverCallback, consumerTag -> { });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println("destination_file Received '" + message + "'");
                };
                channel.basicConsume(QUEUE_NAME_2, true, deliverCallback, consumerTag -> { });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }


}
