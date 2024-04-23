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
import com.rabbitmq.client.impl.AMQImpl;

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
 */
public class RabbitMQConsumerTest {

    private final static String QUEUE_NAME = "work_queues_test";

    public static void main(String[] args) throws Exception {
        Connection connection = RabbitMQFactory.getConnection();
//        Map<String, Object> clientProperties = connection.getClientProperties();
        Channel channel = connection.createChannel();
//        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        channel.queueDeclare(QUEUE_NAME + "_0", true, false, false, null);
        channel.close();
//        channel.queueDeclare(QUEUE_NAME + "_1", true, false, false, null);

//        for (int i = 0; i < 100*100; i++) {
//            channel.queueDeclare(QUEUE_NAME + "_" + i, true, false, false, null);
//        }

        new Thread(new Worker(connection, 0), "hzk-consumer").start();
//        new Thread(new Worker(connection,channel,1), "hzk-consumer").start();

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
                channel.basicQos(2);
                System.out.println("消费者-" + index + " 开始接受消息，channelNumber:" + channel.getChannelNumber());
                RabbitConsumer rabbitConsumer = new RabbitConsumer(connection, channel, QUEUE_NAME + "_" + index);
//                channel.basicConsume(QUEUE_NAME + "_" + index, false, rabbitConsumer);

                Map<String, Object> argMap = new HashMap<>();
                argMap.put("appId", "bos");
//                channel.basicConsume(QUEUE_NAME + "_" + index, false, argMap, rabbitConsumer);
                channel.basicConsume(QUEUE_NAME + "_" + index, false, rabbitConsumer);

//                new Thread(()->{
//                    try {
//                        Thread.currentThread().sleep(1000 * 30);work_queues_test
//                        channel.basicConsume(QUEUE_NAME, false, rabbitConsumer);
//                    } catch (Exception e) {
//
//                    }
//                }).start();

//                while (true) {
//                    Thread.currentThread().sleep(1000 * 3);
//                    long consumerCount = channel.consumerCount(QUEUE_NAME + "_" + index);
//                    System.out.println(QUEUE_NAME + "_" + index + ":" + consumerCount);
//                    if (!channel.isOpen() || consumerCount == 0) {
//                        ShutdownSignalException shutdownSignalException = channel.getCloseReason();
//                        if (shutdownSignalException != null) {
//                            shutdownSignalException.printStackTrace();
//                        }
//
//                        Thread.currentThread().sleep(1000 * 10);
//                        channel = connection.createChannel();
//                        rabbitConsumer.setChannel(channel);
//                        channel.basicConsume(QUEUE_NAME + "_" + index, false, rabbitConsumer);
//                    }
//                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}


class RabbitConsumer extends DefaultConsumer {


    private static ExecutorService threadPool = Executors.newFixedThreadPool(8);
    private Connection connection;
    private String queueName;
    private Channel channel;
    /**
     * Constructs a new instance and records its association to the passed-in channel.
     *
     * @param channel the channel to which this consumer is attached
     */
    public RabbitConsumer(Connection connection, Channel channel, String queueName) {
        super(channel);
        this.connection = connection;
        this.channel = channel;
        this.queueName = queueName;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    @Override
    public Channel getChannel() {
        return channel;
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
            throws IOException {
        String messageId = String.valueOf(envelope.getDeliveryTag());
        boolean isRedeliver = envelope.isRedeliver();
        try {
            super.handleDelivery(consumerTag, envelope, properties, body);


            long deliveryTag = envelope.getDeliveryTag();
            System.out.println("receive message:" + deliveryTag + ",date:" + LocalTime.now());
            Thread.sleep(1000 * 130);
//            channel.basicAck(deliveryTag, false);
            channel.basicAck(deliveryTag, false);
            System.out.println("ack message:" + deliveryTag + ",date:" + LocalTime.now());

//            executorService.execute(()->{
//                try {
//                    String message = new String(body, "UTF-8");
//                    System.out.println(Thread.currentThread().getName() + ",queue=" + this.queueName + ",消费者 Received '" + message + "'");
//                    Thread.sleep(1000 * 10);
//                    System.out.println(Thread.currentThread().getName() + ",queue=" + this.queueName + ",消费者 Received '" + message + "',准备ack");
//                    channel.basicAck(envelope.getDeliveryTag(), false);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//
//            });

//            String message = new String(body, "UTF-8");
//            System.out.println(Thread.currentThread().getName() + ",queue=" + this.queueName + ",消费者 Received '" + message + "'");
//            channel.basicReject(envelope.getDeliveryTag(), true);
//            Thread.sleep(1000 * 10);
//            Random random = new Random();
//            if (random.nextInt(10) > 5) {
//                channel.basicAck(envelope.getDeliveryTag(), false);
//            } else {
//                channel.basicReject(envelope.getDeliveryTag(), true);
//            }


//            channel.basicReject(envelope.getDeliveryTag(), true);
//            threadPool.execute(new Runnable() {
//                @Override
//                public void run() {
//                    try {
//                        System.out.println(Thread.currentThread().getName() + " sleep");
//                        channel.basicAck(envelope.getDeliveryTag(), false);
//                        Thread.sleep(1000 * 20);
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
//            });

//            if (message.contains("5")) {
////                LockSupport.parkNanos(500000000);//500ms
//                channel.basicAck(envelope.getDeliveryTag(), false);
////                channel.basicReject(envelope.getDeliveryTag(), false);
//            } else {
//                // discard
////                channel.basicReject(envelope.getDeliveryTag(), false);
//                // ack
//              channel.basicAck(envelope.getDeliveryTag(), false);
//            }


        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException e) {
        super.handleShutdownSignal(consumerTag, e);
        e.printStackTrace();
        int replyCode = ((AMQImpl.Channel.Close) e.getReason()).getReplyCode();
        // channel被动关闭
        if (replyCode != 200) {
            // 关闭原channel
            int channelNumber = this.channel.getChannelNumber();
            System.out.println("oldChannelNumber:" + channelNumber);
            System.out.println("isOpen:" + this.channel.isOpen());
            try {
                // 重建channel
                this.channel = connection.createChannel();
                int newChannelNumber = this.channel.getChannelNumber();
                System.out.println("newChannelNumber:" + newChannelNumber);
                channel.basicQos(100);
                channel.basicConsume(this.queueName, false, this);
                System.out.println("handleShutdownSignal重启消费者，channelNumber:" + channel.getChannelNumber());
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }

    }


    @Override
    public void handleConsumeOk(String consumerTag) {
        super.handleConsumeOk(consumerTag);
    }

    @Override
    public void handleCancelOk(String consumerTag) {
        super.handleCancelOk(consumerTag);
    }

    @Override
    public void handleCancel(String consumerTag) throws IOException {
        super.handleCancel(consumerTag);
    }

    @Override
    public void handleRecoverOk(String consumerTag) {
        super.handleRecoverOk(consumerTag);
    }

    @Override
    public String getConsumerTag() {
        return super.getConsumerTag();
    }
}