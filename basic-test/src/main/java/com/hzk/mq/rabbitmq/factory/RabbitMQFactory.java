package com.hzk.mq.rabbitmq.factory;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class RabbitMQFactory {

    private static ConnectionFactory factory;

    private static final Object LOCKER = new Object();

    public static Connection getConnection() throws Exception {
        if (factory == null) {
            synchronized (LOCKER) {
                if (factory == null) {
                    factory = new ConnectionFactory();
                    factory.setHost("localhost");
                    factory.setPort(5672);
                    factory.setUsername("guest");
                    factory.setPassword("guest");
                    // 心跳间隔（秒），默认60s
                    factory.setRequestedHeartbeat(5);
                    factory.setThreadFactory(new ThreadFactory() {
                        private final AtomicInteger poolNumber = new AtomicInteger(1);
                        private final AtomicInteger threadNumber = new AtomicInteger(1);
                        SecurityManager s = System.getSecurityManager();
                        private final ThreadGroup group = (s != null) ? s.getThreadGroup() :
                                Thread.currentThread().getThreadGroup();
                        @Override
                        public Thread newThread(Runnable r) {
                            String namePrefix = "hzk-pool-" +
                                    poolNumber.getAndIncrement() +
                                    "-thread-";
                            Thread t = new Thread(group, r,
                                    namePrefix + threadNumber.getAndIncrement(),
                                    0);
                            return t;
                        }
                    });
                }
            }
        }
        return factory.newConnection();
    }



}
