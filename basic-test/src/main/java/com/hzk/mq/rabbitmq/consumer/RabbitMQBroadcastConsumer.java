package com.hzk.mq.rabbitmq.consumer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * rabbitmq纯广播模式，消费者
 */
public class RabbitMQBroadcastConsumer {


    protected final static String EXCHANGE_NAME = "exchange_fanout_hzk";

    public static void main(String[] args) throws Exception {
        Connection connection = null;
        try {
            connection = RabbitMQFactory.getConnection();
            Channel channel = connection.createChannel();

            //声明exchange,类型为fanout,消息路由到所有绑定的队列，广播模式
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

//            String queueName = channel.queueDeclare().getQueue();
            // 声明队列
            Map<String, Object> queueArgumentMap = new HashMap<>();
            queueArgumentMap.put("x-message-ttl", 10000);// 单位毫秒
            queueArgumentMap.put("appId", "bos");
            String queueName = channel.queueDeclare("", false, true, true, queueArgumentMap).getQueue();
            // exchange绑定
            Map<String, Object> exchangeArgumentMap = new HashMap<>();
            exchangeArgumentMap.put("appId", "bos");
            channel.queueBind(queueName, EXCHANGE_NAME, "", exchangeArgumentMap);

            DefaultConsumer consumer = new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    System.out.println("接收数据:" + new String(body));
                    try {
                        Thread.currentThread().sleep(1000 * 130);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            };



            String result = channel.basicConsume(queueName, false, consumer);
//            Thread.currentThread().sleep(1000 * 10);
//            // 报错ShutdownSignalException
//            try {
//                channel.queueDeclare("amq.queue", false, false, false,null).getConsumerCount();
//            }catch (Exception e) {
//                e.printStackTrace();
//            }
//            String result2 = channel.basicConsume(queueName, false, consumer);
            System.out.println("getChannelNumber=" + channel.getChannelNumber());
//            Thread.currentThread().sleep(1000 * 15);


            long consumerCount = channel.consumerCount(queueName);
            System.out.println(consumerCount);

//            String consumerTag = consumer.getConsumerTag();
//            channel.basicCancel(consumerTag);

            System.in.read();

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}

