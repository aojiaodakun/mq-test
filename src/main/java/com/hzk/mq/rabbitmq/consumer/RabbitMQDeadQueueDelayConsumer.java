package com.hzk.mq.rabbitmq.consumer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.util.HashMap;
import java.util.Map;

/**
 * 基于死信队列
 * 实现延迟功能，消费者
 */
public class RabbitMQDeadQueueDelayConsumer {

    // 普通队列
    private final static String NORMAL_QUEUE = "normal_queue";
    // 普通交换机
    private final static String NORMAL_EXCHANGE = "normal_exchange";
    // 死信队列
    private final static String DEAD_QUEUE = "dead_queue";
    // 死信交换机
    private final static String DEAD_EXCHANGE = "dead_exchange";



    public static void main(String[] args) throws Exception {
        Connection connection = RabbitMQFactory.getConnection();
        Channel channel = connection.createChannel();

        // 声明交换机
        channel.exchangeDeclare(NORMAL_EXCHANGE, BuiltinExchangeType.DIRECT);
        channel.exchangeDeclare(DEAD_EXCHANGE, BuiltinExchangeType.DIRECT);

        // 声明队列
        Map<String, Object> ttlQueueArg = new HashMap<>();
//        ttlQueueArg.put("x-message-ttl", 10000);
        ttlQueueArg.put("x-dead-letter-exchange", DEAD_EXCHANGE);
        ttlQueueArg.put("x-dead-letter-routing-key", "dead");
        channel.queueDeclare(NORMAL_QUEUE, true, false, false, ttlQueueArg);
        channel.queueDeclare(DEAD_QUEUE, true, false, false, null);

        // 交换机与队列绑定
        channel.queueBind(NORMAL_QUEUE, NORMAL_EXCHANGE, "normal");
        channel.queueBind(DEAD_QUEUE, DEAD_EXCHANGE, "dead");


        // 正常消费者，不启动，则ttl过期消息会路由到死信队列


        // 死信消费者
        System.out.println("死信消费者开始接受消息。");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("死信消费者 Received '" + message + "'");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };
        //自动确认,如果业务处理失败或该消费者宕机,发送到该消费者的消息都会被删除
        channel.basicConsume(DEAD_QUEUE, true, deliverCallback, consumerTag -> { });

    }

}
