package com.hzk.mq.rabbitmq.producer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * 性能问题测试
 */
public class RabbitMQProducerPerfTest {

    private final static String QUEUE_NAME = "work_queues_test_0";

    public static void main(String[] args) throws Exception{
        Connection connection = RabbitMQFactory.getConnection();
        for (int i = 0; i < 1; i++) {
            new Thread(()->{
                List<Channel> channelList = new ArrayList<>(5000);
                for (int j = 0; j < 5000; j++) {
                    Channel channel = null;
                    try {
                        channel = connection.createChannel();
                        channelList.add(channel);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
//                        try {
//                            channel.close();
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
                    }

                }
            },"thread_" + i).start();
        }


//        for (int i = 0; i < 50; i++) {
//            new Thread(()->{
//                for (int j = 0; j < 5000; j++) {
//                    Channel channel = null;
//                    try {
//                        channel = connection.createChannel();
//                        String message = "消息-" + j;
//                        channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    } finally {
//                        try {
//                            channel.close();
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    }
//
//                }
//            },"thread_" + i).start();
//        }

        System.in.read();

    }

}
