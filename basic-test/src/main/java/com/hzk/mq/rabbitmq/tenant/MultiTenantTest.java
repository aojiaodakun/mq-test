package com.hzk.mq.rabbitmq.tenant;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 单机rabbitmq，多租户测试
 * 50租户*300队列=15000队列
 * 50个vhost，50个connection，300个队列，15000个消费者
 */
public class MultiTenantTest {

    private static List<String> vhostList = new ArrayList<>(50);

    private static Map<String, Connection> vhost_conn_map = new HashMap<>(64);

    private static final int tenantSize = 50;
    private static final int queueSize = 300;

    public static void main(String[] args) throws Exception {
        // vhost
        for (int i = 1; i <= tenantSize; i++) {
            vhostList.add("vhost_" + i);
        }
        for(String tempVhost : vhostList) {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            factory.setPort(5672);
            factory.setVirtualHost(tempVhost);
            factory.setUsername("guest");
            factory.setPassword("guest");
            Connection connection = factory.newConnection();
            vhost_conn_map.put(tempVhost, connection);
            Channel channel = connection.createChannel();
            for (int i = 1; i <= queueSize; i++) {
                String queueName = "queue-" + i;
                // 声明队列
                channel.queueDeclare(queueName, true, false, false, null);
                // 启动消费者
                Channel consumerChannel = connection.createChannel();
                RabbitConsumer rabbitConsumer = new RabbitConsumer(tempVhost,queueName, consumerChannel);
                consumerChannel.basicConsume(queueName, false, rabbitConsumer);
                Thread.currentThread().sleep( 10);
            }
            channel.close();
        }
        System.in.read();

        Random random = new Random();
        // 随机启动生产者
        while (true) {
            Thread.currentThread().sleep( 10);
            for(String tempVhost : vhostList) {
                int index = random.nextInt(300);
                String queueName = "queue-" + index;
                Connection connection = vhost_conn_map.get(tempVhost);
                Channel channel = connection.createChannel();
                String message = "消息-" + index;
                channel.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
                channel.close();
                Thread.currentThread().sleep( 10);
            }
        }

    }

}
class RabbitConsumer extends DefaultConsumer {
    private String vhost;
    private String queueName;
    private Channel channel;
    public RabbitConsumer(String vhost, String queueName, Channel channel) {
        super(channel);
        this.vhost = vhost;
        this.queueName = queueName;
        this.channel = channel;
    }
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
            throws IOException {
        try {
            super.handleDelivery(consumerTag, envelope, properties, body);
            String message = new String(body, "UTF-8");
            System.out.println(Thread.currentThread().getName() +
                    ",vhost=" +this.vhost +
                    ",queue=" + this.queueName +
                    ",Received=" + message);
            channel.basicAck(envelope.getDeliveryTag(), false);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}