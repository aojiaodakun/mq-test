package com.hzk.mq.rabbitmq.consumer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.impl.AMQConnection;

import java.util.Map;

/**
 * Work Queues(工作队列模式)，一个生产者，多个消费者，一条消息只能被一个消费者消费
 */
public class RabbitMQWorkQueuesConsumer {

    private final static String QUEUE_NAME = "work_queues";

    public static void main(String[] args) throws Exception {
        Connection connection = RabbitMQFactory.getConnection();
        Map<String, Object> clientProperties = connection.getClientProperties();


        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        //模拟三个消费者,rabbitmq默认会把消息轮询推给每个消费者
            new Thread(new Worker(connection, 1)).start();
            new Thread(new Worker2(connection, 2)).start();
//            new Thread(new Worker3(connection, 3)).start();

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
                System.out.println("消费者-" + index + " 开始接受消息。");
                Channel channel = connection.createChannel();

                channel.basicQos(1);//一次只接受一条消息
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    try {
                        String message = new String(delivery.getBody(), "UTF-8");
                        System.out.println("消费者-" + index + " Received '" + message + "'");
                        //业务处理...
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        //在业务处理完成后手动确认；避免一个消费者宕机等导致消息丢失
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    }
                };
                //autoAck设为false
                channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 手动确认Worker
     */
    static class Worker2 implements Runnable {
        private Connection connection;
        private int index;

        public Worker2(Connection connection, int index) {
            this.connection = connection;
            this.index = index;
        }

        @Override
        public void run() {
            try {
                System.out.println("消费者-" + index + " 开始接受消息。");
                Channel channel = connection.createChannel();

                channel.basicQos(1);//一次只接受一条消息
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    try {
                        String message = new String(delivery.getBody(), "UTF-8");
                        System.out.println("消费者-" + index + " Received '" + message + "'");
                        //业务处理...
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        //在业务处理完成后手动确认；避免一个消费者宕机等导致消息丢失
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    }
                };
                //autoAck设为false
                channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 拉模式消费
     */
    static class Worker3 implements Runnable {
        private Connection connection;
        private int index;

        public Worker3(Connection connection, int index) {
            this.connection = connection;
            this.index = index;
        }

        @Override
        public void run() {
            try {
                System.out.println("消费者-" + index + " 开始接受消息。");
                Channel channel = connection.createChannel();

                while (true) {
                    GetResponse response = channel.basicGet(QUEUE_NAME, false);
                    if (response == null) {
                        continue;
                    }
                    String message = new String(response.getBody());
                    System.out.println("消费者-" + index + " Received '" + message + "'");
                    //业务处理...
                    Thread.sleep(1000);
                    channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}
