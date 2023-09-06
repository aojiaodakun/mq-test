package com.hzk.mq.rabbitmq.consumer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.util.HashMap;
import java.util.Map;

/**
 * 基于插件
 * 实现延迟功能，消费者
 */
public class RabbitMQPluginDelayConsumer {

    // 延迟交换机
    private final static String DELAY_EXCHANGE = "plugin_delay_exchange";
    // 延迟队列
    private final static String DELAY_QUEUE = "plugin_delay_queue";

    public static void main(String[] args) throws Exception {
        Connection connection = RabbitMQFactory.getConnection();
        Channel channel = connection.createChannel();

        // 声明交换机，延迟类型
        Map<String, Object> argsMap = new HashMap<>();
        argsMap.put("x-delayed-type", "direct");
        channel.exchangeDeclare(DELAY_EXCHANGE, "x-delayed-message", true, false, argsMap);

        // 声明队列
        channel.queueDeclare(DELAY_QUEUE, true, false, false, null);

        // 交换机与队列绑定
        channel.queueBind(DELAY_QUEUE, DELAY_EXCHANGE, "delay");


        // 延迟队列消费者
        System.out.println("延迟队列消费者开始接受消息。");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("延迟队列消费者 Received '" + message + "'");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };
        //自动确认,如果业务处理失败或该消费者宕机,发送到该消费者的消息都会被删除
        channel.basicConsume(DELAY_QUEUE, true, deliverCallback, consumerTag -> { });

    }

}
