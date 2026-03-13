package com.hzk.mq.rabbitmq.factory;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * rabbitmq集群模式获取连接
 */
public class RabbitMQClusterFactory {

    private static ExecutorService executorService = Executors.newFixedThreadPool(10);

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
//        factory.setHost("localhost");
//        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        List<Address> addressList = new ArrayList<>();
        Address address1 = new Address("localhost", 5672);
        Address address2 = new Address("localhost", 5672);
        Address address3 = new Address("localhost", 5672);
        addressList.add(address1);
        addressList.add(address2);
        addressList.add(address3);
//        Connection connection = factory.newConnection();
        Connection connection = factory.newConnection(executorService, addressList);
        System.out.println(connection.isOpen());

    }

}
