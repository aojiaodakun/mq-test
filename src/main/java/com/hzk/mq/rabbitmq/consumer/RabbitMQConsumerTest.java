package com.hzk.mq.rabbitmq.consumer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;

import java.io.IOException;
import java.util.Map;

/**
 * Work Queues(工作队列模式)，一个生产者，多个消费者，一条消息只能被一个消费者消费
 */
public class RabbitMQConsumerTest {

    private final static String QUEUE_NAME = "work_queues_test";

    public static void main(String[] args) throws Exception {
        Connection connection = RabbitMQFactory.getConnection();
        Map<String, Object> clientProperties = connection.getClientProperties();


        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        new Thread(new Worker(connection, 1)).start();

        System.in.read();
    }

    /**
     * 自动确认Worker
     */
    static class Worker implements Runnable {
        private Connection connection;
        private int index;

        public Worker(Connection connection, int index) {
            this.connection = connection;
            this.index = index;
        }

        @Override
        public void run() {
            try {
                Channel channel = connection.createChannel();
                System.out.println("消费者-" + index + " 开始接受消息。");
                RabbitConsumer rabbitConsumer = new RabbitConsumer(channel);
                channel.basicConsume(QUEUE_NAME, false, rabbitConsumer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}


class RabbitConsumer extends DefaultConsumer {


    /**
     * Constructs a new instance and records its association to the passed-in channel.
     *
     * @param channel the channel to which this consumer is attached
     */
    public RabbitConsumer(Channel channel) {
        super(channel);
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
            throws IOException {
        Channel channel = getChannel();
        String messageId = String.valueOf(envelope.getDeliveryTag());
        boolean isRedeliver = envelope.isRedeliver();
        try {
            super.handleDelivery(consumerTag, envelope, properties, body);
            String message = new String(body, "UTF-8");
            System.out.println("消费者 Received '" + message + "'");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            channel.basicAck(envelope.getDeliveryTag(), false);

//            if (message.contains("hzk")) {
//                channel.basicReject(envelope.getDeliveryTag(), true);
//            } else {
//                // discard
//                channel.basicReject(envelope.getDeliveryTag(), false);
//                // ack
////              channel.basicAck(envelope.getDeliveryTag(), false);
//            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}