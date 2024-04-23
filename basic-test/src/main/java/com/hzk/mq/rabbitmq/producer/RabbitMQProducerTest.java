package com.hzk.mq.rabbitmq.producer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

import java.util.HashMap;
import java.util.Map;

/**
 *  rabbitmq.conf
 *  channel_max = 4095
 *  max_message_size=234217728，默认128m，最大不能超过512m
 *  1、单个消息最大值
 *  服务端：max_message_size
 */
public class RabbitMQProducerTest {

    private final static String QUEUE_NAME = "work_queues_test_0";

    public static void main(String[] args) throws Exception{
        Connection connection = null;
        Channel channel = null;
        try {
            connection = RabbitMQFactory.getConnection();
            channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            
            Map<String, Object> argMap = new HashMap<>();
            argMap.put("appId", "bos1");

            for (int i = 0; i < 1; i++) {
                AMQP.BasicProperties basicProperties = new AMQP.BasicProperties("text/plain",
                        null,
                        argMap,
                        2,
                        0, null, null, null,
                        null, null, null, null,
                        null, null);
                String message = "消息-" + i;
                channel.basicPublish("", QUEUE_NAME, basicProperties, message.getBytes());
                System.out.println(" [x] Sent '" + message + "'");
            }

//            channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, "hzk666".getBytes());
//            System.out.println(" [x] Sent '" + "hzk666" + "'");
//
//            for (int i = 10; i < 15; i++) {
//                String message = "消息-" + i;
//                //发送的消息持久化,重启rabbitmq消息也不会丢失
//                channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
//                System.out.println(" [x] Sent '" + message + "'");
//            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            channel.close();
            connection.close();
        }
    }

}
