package com.hzk.mq.rabbitmq.producer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

/**
 * Work Queues(工作队列模式)，一个生产者，多个消费者，一条消息只能被一个消费者消费
 */
public class RabbitMQWorkQueuesProducer {

    private final static String QUEUE_NAME = "work_queues";

    public static void main(String[] args) throws Exception{
        Connection connection = null;
        Channel channel = null;
        try {
            connection = RabbitMQFactory.getConnection();
            channel = connection.createChannel();
            for (int i = 0; i < 10; i++) {
                String message = "消息-" + i;
                //发送的消息持久化,重启rabbitmq消息也不会丢失
                channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
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
