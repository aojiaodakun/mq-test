package com.hzk.mq.rabbitmq.factory;

import com.caucho.hessian.io.Hessian2Output;
import com.caucho.hessian.io.Serializer;
import com.caucho.hessian.io.SerializerFactory;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class RabbitConnTest {

    private static ConcurrentHashMap<String, Connection> poolMap = new ConcurrentHashMap<>();
    private static final int maxConnectionSize = 2;

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 20; i++) {
            Connection connection = getConnectionByRegion(i+"");
        }
        Connection connection = getConnectionByRegion("a");
        Channel channel = connection.createChannel();
        //声明exchange,类型为fanout,消息路由到所有绑定的队列，广播模式
        channel.exchangeDeclare("exchange_fanout_hzk", BuiltinExchangeType.FANOUT);
        // 临时队列
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, "exchange_fanout_hzk", "");

//        Connection connection1 = RabbitMQFactory.getConnection();

//        new Thread(()->{
//            try {
//                Thread.sleep(1000 * 30);
//                System.out.println("sssssssssss");
//                connection.close();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }).start();

//        System.in.read();

        while (true) {
            try {
                getConnectionByRegion("a");
            } catch (Exception e) {
                e.printStackTrace();
            }
            Thread.sleep(100);
        }


    }


    public static Connection getConnectionByRegion(String region) {
        Connection con = null;
        String key;
        String regionServerKey = "mq.server";
        int index = Math.abs(region.hashCode() % maxConnectionSize);
        key = regionServerKey + "_" + index;
        if (poolMap.containsKey(key)) {
            con = poolMap.get(key);
            if (con.isOpen()) {
                return con;
            }
        }
        synchronized (RabbitConnTest.class) {
            if (poolMap.containsKey(key)) {
                con = poolMap.get(key);
                if (con.isOpen()) {
                    return con;
                } else {
                    // 连接泄露问题bug
//                    try {
//                        poolMap.remove(key);
//                        con.close();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
                }
            }
            try {
                Connection client = RabbitMQFactory.getConnection();
                poolMap.put(key, client);
                return client;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return con;
    }

}
