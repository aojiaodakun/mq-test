package com.hzk.mq.rabbitmq.consumer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

/**
 * Work Queues(工作队列模式)，一个生产者，多个消费者，一条消息只能被一个消费者消费
 * 拉模式
 */
public class RabbitMQPullConsumerTest {

    private final static String QUEUE_NAME = "work_queues_test";

    public static void main(String[] args) throws Exception {
        Connection connection = RabbitMQFactory.getConnection();
        Channel channel0 = connection.createChannel();
        String queueName = QUEUE_NAME + "_0";
        channel0.queueDeclare(queueName, true, false, false, null);
        channel0.close();

        // 消费者
        Channel channel = connection.createChannel();
        channel.basicQos(100);
        while (channel.isOpen()) {
            GetResponse response = channel.basicGet(queueName, false);
            if(response == null){
                Thread.sleep(100);
                continue;
            }
            long deliveryTag = response.getEnvelope().getDeliveryTag();
            channel.basicAck(deliveryTag, false);
            System.out.println("ack message:" + deliveryTag + ",date:" + LocalTime.now());
        }
        System.in.read();
    }

}
