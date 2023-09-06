package com.hzk.mq.rabbitmq.consumer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;

/**
 * Routing(路由模式),队列通过bingingKey绑定到Exchange,发送消息时指定routingKey,Exchange根据routingKey精确匹配bindingKey来路由消息
 */
public class RabbitMQRoutingConsumer {

    private final static String QUEUE_NAME_1 = "direct_destination_terminal";
    private final static String QUEUE_NAME_2 = "direct_destination_file";

    public static void main(String[] args) throws Exception {
        Connection connection = RabbitMQFactory.getConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME_1, true, false, false, null);
        channel.queueDeclare(QUEUE_NAME_2, true, false, false, null);
        System.out.println("开始接受消息...");
        new Thread(() -> {
            try {
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println("direct_destination_terminal Received '" + delivery.getEnvelope().getRoutingKey() + ":" + message + "'");
                };
                channel.basicConsume(QUEUE_NAME_1, true, deliverCallback, consumerTag -> { });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    System.out.println("direct_destination_file Received '" + delivery.getEnvelope().getRoutingKey() + ":" + message + "'");
                };
                channel.basicConsume(QUEUE_NAME_2, true, deliverCallback, consumerTag -> { });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

    }

}
