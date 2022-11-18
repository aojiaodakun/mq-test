package com.hzk.mq.rabbitmq.consumer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

/**
 * HelloWorld，一个生产者，一个消费者
 */
public class RabbitMQHelloWorldConsumer {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws Exception {
        Connection connection = RabbitMQFactory.getConnection();
        Channel channel = connection.createChannel();
        /**
         * 声明队列,如果队列不存在则创建;如果已存在则设置的参数值需跟原队列一致,否则会保持
         * 默认绑定到默认队列，routingKey就是队列名称
         *
         * 是否持久化: 如果为false,则重启rabbit后,队列会消失
         * 是否排他: 即只允许该channel访问该队列,一般等于true的话用于一个队列只能有一个消费者来消费的场景
         * 是否自动删除: 消费完消息删除该队列
         * 其他属性：x-queue-type(quorum、classic)，默认为classic
         */
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
        };
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
    }


}
