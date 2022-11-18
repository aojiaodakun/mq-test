package com.hzk.mq.rabbitmq.factory;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

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
                }
            }
        }
        return factory.newConnection();
    }



}
