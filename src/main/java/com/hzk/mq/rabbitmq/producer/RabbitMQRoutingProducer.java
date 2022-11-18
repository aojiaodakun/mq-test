package com.hzk.mq.rabbitmq.producer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * Routing(路由模式),队列通过bingingKey绑定到Exchange,发送消息时指定routingKey,Exchange根据routingKey精确匹配bindingKey来路由消息
 */
public class RabbitMQRoutingProducer {

    private static final String EXCHANGE_NAME = "direct_logs";
    private final static String QUEUE_NAME_1 = "direct_destination_terminal";
    private final static String QUEUE_NAME_2 = "direct_destination_file";

    public static void main(String[] args) throws Exception{
        Connection connection = null;
        Channel channel = null;
        try {
            connection = RabbitMQFactory.getConnection();
            channel = connection.createChannel();
            //声明exchange,类型为direct,根据routingKey精确匹配bindingKey来路由消息
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
            //队列1接受info、warn、error日志
            channel.queueBind(QUEUE_NAME_1, EXCHANGE_NAME, "info");
            channel.queueBind(QUEUE_NAME_1, EXCHANGE_NAME, "warn");
            channel.queueBind(QUEUE_NAME_1, EXCHANGE_NAME, "error");
            //队列2只接受error日志
            channel.queueBind(QUEUE_NAME_2, EXCHANGE_NAME, "error");
            for (int i = 0; i < 20; i++) {
                String message = "消息-" + i;
                String routingKey = "";
                if (i % 3 == 0) {
                    routingKey = "info";
                } else if (i % 3 == 1) {
                    routingKey = "warn";
                } else if (i % 3 == 2) {
                    routingKey = "error";
                }
                channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
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
