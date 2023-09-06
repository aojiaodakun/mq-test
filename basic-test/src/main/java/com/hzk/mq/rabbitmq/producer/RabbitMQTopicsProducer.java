package com.hzk.mq.rabbitmq.producer;


import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * Topics(主题模式),队列通过bingingKey绑定到Exchange,发送消息时指定routingKey,Exchange根据根据routingKey匹配bindingKey模式来路由消息
 * bingingKey模式用"."分隔,"*"代表一个单词,"#"代表0或多个单词
 */
public class RabbitMQTopicsProducer {

    private static final String EXCHANGE_NAME = "topics_logs";
    private final static String QUEUE_NAME_1 = "topics_destination_terminal";
    private final static String QUEUE_NAME_2 = "topics_destination_file";

    public static void main(String[] args) throws Exception{
        Connection connection = null;
        Channel channel = null;
        try {
            connection = RabbitMQFactory.getConnection();
            channel = connection.createChannel();
            //队列1接受模块A的所有日志
            channel.queueBind(QUEUE_NAME_1, EXCHANGE_NAME, "moduleA.*");
            //队列2接受所有error的日志
            channel.queueBind(QUEUE_NAME_2, EXCHANGE_NAME, "*.error");
            for (int i = 0; i < 20; i++) {
                String message = "消息-" + i;
                String routingKey = "";
                if (i % 2 == 0) {
                    routingKey = "moduleA";
                } else {
                    routingKey = "moduleB";
                }
                routingKey += ".";
                if (i % 3 == 0) {
                    routingKey += "info";
                } else if (i % 3 == 1) {
                    routingKey += "warn";
                } else if (i % 3 == 2) {
                    routingKey += "error";
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
