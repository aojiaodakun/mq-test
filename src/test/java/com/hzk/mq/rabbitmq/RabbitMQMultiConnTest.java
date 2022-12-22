package com.hzk.mq.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.MessageProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 多个Conn对象测试，一个conn最大channel为2048
 */
public class RabbitMQMultiConnTest {

    private final static String QUEUE_NAME = "work_queues";

    private ConnectionFactory factory;

    private static final Object LOCKER = new Object();

    private int connSize = 3;
    private List<Connection> connectionList = new ArrayList<>(connSize);

    @Before
    public void before() throws IOException, TimeoutException {
        factory = getConnectionFactory();
    }

    @After
    public void after() throws Exception {
        for(Connection conn : connectionList) {
            conn.close();
        }
    }


    @Test
    public void consumerTest() throws Exception{
        // 消费者工作线程池
        ExecutorService executorService = Executors.newFixedThreadPool(5, new ThreadFactory() {
            private AtomicInteger atomicInteger = new AtomicInteger(0);
            public Thread newThread(Runnable r) {
                return new Thread(r, "hzk-rabbit-pool-" + this.atomicInteger.incrementAndGet());
            }
        });
        Connection connection = factory.newConnection(executorService);

        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        try {
            System.out.println(Thread.currentThread().getName() + ",消费者A开始接受消息。");
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(Thread.currentThread().getName() + ",消费者A-Received '" + message + "'");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            };
            //自动确认,如果业务处理失败或该消费者宕机,发送到该消费者的消息都会被删除
            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
        } catch (Exception e) {
            e.printStackTrace();
        }

        Channel channel2 = connection.createChannel();
        try {
            System.out.println(Thread.currentThread().getName() + ",消费者B开始接受消息。");
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(Thread.currentThread().getName() + ",消费者B-Received '" + message + "'");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            };
            //自动确认,如果业务处理失败或该消费者宕机,发送到该消费者的消息都会被删除
            channel2.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.in.read();
    }

    @Test
    public void producerTest() throws Exception{
        for (int i = 0; i < connSize; i++) {
            connectionList.add(factory.newConnection());
        }
        Connection connection = null;
        Channel channel = null;
        try {
            for (int i = 0; i < 20; i++) {
                int index = i%3;
                connection = connectionList.get(index);
                channel = connection.createChannel();
                String message = "消息-" + i;
                //发送的消息持久化,重启rabbitmq消息也不会丢失
                channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
                System.out.println(" [x] Sent '" + message + "'");
                channel.close();
                Thread.currentThread().sleep(1000 * 1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private ConnectionFactory getConnectionFactory() {
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
        return factory;
    }

}
