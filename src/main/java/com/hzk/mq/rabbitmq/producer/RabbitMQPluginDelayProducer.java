package com.hzk.mq.rabbitmq.producer;


import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * 基于插件
 * 实现延迟功能，生产者
 */
public class RabbitMQPluginDelayProducer {

    // 延迟交换机
    private final static String DELAY_EXCHANGE = "plugin_delay_exchange";
    // 延迟队列
    private final static String DELAY_QUEUE = "plugin_delay_queue";


    public static void main(String[] args) throws Exception {
        Connection connection = null;
        Channel channel = null;
        try {
            connection = RabbitMQFactory.getConnection();
            channel = connection.createChannel();


            String routingKey = "delay";
            AMQP.BasicProperties properties1 = new AMQP.BasicProperties().builder()
                    .expiration("10000")
                    .build();
            for (int i = 0; i < 1; i++) {
                String message = "10000_info" + i;
                channel.basicPublish(DELAY_EXCHANGE, routingKey, properties1, message.getBytes());
                System.out.println(" [x] Sent '" + routingKey + ":" + message + "'");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            channel.close();
            connection.close();
        }
    }
}
