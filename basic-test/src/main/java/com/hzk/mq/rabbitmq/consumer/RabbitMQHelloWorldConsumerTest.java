package com.hzk.mq.rabbitmq.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HelloWorld，一个生产者，一个消费者
 */
public class RabbitMQHelloWorldConsumerTest {

    private static ConnectionFactory factory;

    private static final Object LOCKER = new Object();

    private final static String QUEUE_NAME = "hello";

    private final static ExecutorService threadPool = Executors.newFixedThreadPool(10, new ThreadFactory() {
        AtomicInteger integer = new AtomicInteger();
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r,"hzk-work-" + integer.incrementAndGet());
        }
    });

    private final static ExecutorService threadPool2 = Executors.newFixedThreadPool(10, new ThreadFactory() {
        AtomicInteger integer = new AtomicInteger();
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r,"RabbitmqAsyncConsumer" + integer.incrementAndGet());
        }
    });

    public static void main(String[] args) throws Exception {
        Connection connection = getConnection();
        Channel channel = connection.createChannel();
        /**
         * 声明队列,如果队列不存在则创建;如果已存在则设置的参数值需跟原队列一致,否则会保持
         * 默认绑定到默认队列，routingKey就是队列名称
         *
         * 是否持久化: 如果为false,则重启rabbit后,队列会消失
         * 是否排他: 即只允许该channel访问该队列,一般等于true的话用于一个队列只能有一个消费者来消费的场景
         * 是否自动删除: 消费完消息删除该队列
         * 其他属性：x-queue-type(quorum、classic)，默认为classic
         */
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        channel.basicQos(2);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {

            try {
                String message = new String(delivery.getBody(), "UTF-8");
//                try {
//                    Thread.currentThread().sleep(1000 * 2);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                String dateString = df.format(new Date());
                System.out.println(" [x] Received '" + message + "'" + ",date:" + dateString);
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
                e.printStackTrace();
            }


//            threadPool2.execute(new Runnable() {
//                @Override
//                public void run() {
//                    try {
//                        String message = new String(delivery.getBody(), "UTF-8");
//                        try {
//                            Thread.currentThread().sleep(1000 * 60);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                        String dateString = df.format(new Date());
//                        System.out.println(" [x] Received '" + message + "'" + ",date:" + dateString);
//                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
//            });
        };
        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });
    }

    public static Connection getConnection() throws Exception {
        if (factory == null) {
            synchronized (LOCKER) {
                if (factory == null) {
                    factory = new ConnectionFactory();
                    factory.setHost("localhost");
                    factory.setPort(5672);
                    factory.setUsername("guest");
                    factory.setPassword("guest");
                }
            }
        }
        return factory.newConnection(threadPool);
    }


}
