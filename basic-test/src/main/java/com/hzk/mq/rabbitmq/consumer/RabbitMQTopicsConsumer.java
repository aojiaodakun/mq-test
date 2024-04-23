package com.hzk.mq.rabbitmq.consumer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;

/**
 * Topics(路由模式),队列通过bingingKey绑定到Exchange,发送消息时指定routingKey,Exchange根据routingKey匹配bindingKey模式来路由消息
 * bingingKey模式用"."分隔,"*"代表一个单词,"#"代表0或多个单词
 */
public class RabbitMQTopicsConsumer {

    private final static String QUEUE_NAME_1 = "topics_destination_terminal";
    private final static String QUEUE_NAME_2 = "topics_destination_file";
    private static final String EXCHANGE_NAME = "topics_logs";

    public static void main(String[] args) throws Exception {
        Connection connection = RabbitMQFactory.getConnection();
        Channel channel = connection.createChannel();

        //声明exchange,类型为topic,根据routingKey匹配bindingKey模式来路由消息
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        channel.queueDeclare(QUEUE_NAME_1, true, false, false, null);
        channel.queueDeclare(QUEUE_NAME_2, true, false, false, null);
        System.out.println("开始接受消息...");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("topics_destination_terminal Received '" + delivery.getEnvelope().getRoutingKey() + ":" + message + "'");
        };
        channel.basicConsume(QUEUE_NAME_1, true, deliverCallback, consumerTag -> { });


        DeliverCallback deliverCallback2 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("topics_destination_file Received '" + delivery.getEnvelope().getRoutingKey() + ":" + message + "'");
        };
        Channel channel2 = connection.createChannel();
        channel2.basicConsume(QUEUE_NAME_2, true, deliverCallback2, consumerTag -> { });

    }

}
