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
//                    factory.setHost("172.20.158.201");
                    factory.setPort(5672);
                    factory.setUsername("admin");
                    factory.setPassword("admin");
                    factory.setVirtualHost("/");
                    /**
                     * 心跳间隔，默认60秒。取客户端和服务端配置的最小值
                     * com.rabbitmq.client.impl.AMQConnection#start
                     * int negotiatedHeartbeat = negotiatedMaxValue(this.requestedHeartbeat, connTune.getHeartbeat());
                     */
                    factory.setRequestedHeartbeat(60);

                    factory.setConnectionTimeout(30000); // 30s
                    factory.setHandshakeTimeout(30000);
                    factory.setChannelRpcTimeout(30000); // 避免queueDeclare超时



//                    factory.setThreadFactory(new ThreadFactory() {
//                        private final AtomicInteger poolNumber = new AtomicInteger(1);
//                        private final AtomicInteger threadNumber = new AtomicInteger(1);
//                        SecurityManager s = System.getSecurityManager();
//                        private final ThreadGroup group = (s != null) ? s.getThreadGroup() :
//                                Thread.currentThread().getThreadGroup();
//                        @Override
//                        public Thread newThread(Runnable r) {
//                            String namePrefix = "hzk-pool-" +
//                                    poolNumber.getAndIncrement() +
//                                    "-thread-";
//                            Thread t = new Thread(group, r,
//                                    namePrefix + threadNumber.getAndIncrement(),
//                                    0);
//                            return t;
//                        }
//                    });
                }
            }
        }
        return factory.newConnection();
    }



}
