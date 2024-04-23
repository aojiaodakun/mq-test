package com.hzk.mq.rabbitmq;

import com.hzk.mq.rabbitmq.factory.RabbitMQChannelPool;
import com.rabbitmq.client.Channel;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class RabbitMQChannelPoolTest {

    private final static String QUEUE_NAME = "work_queues_test_0";

    public static void main(String[] args) throws Exception {
        // 创建Apache Commons Pool2配置
        GenericObjectPoolConfig<Channel> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMinIdle(10);// 最小空闲数
        poolConfig.setMaxIdle(20);// 最大空闲数
        poolConfig.setMaxTotal(100);// 最大数
        poolConfig.setTestOnBorrow(true);// 在从对象池获取对象时是否检测对象有效
        poolConfig.setMaxWait(Duration.of(10, ChronoUnit.SECONDS));// 最大等待时长

        RabbitMQChannelPool factory = new RabbitMQChannelPool();

        GenericObjectPool<Channel> myPool = new GenericObjectPool<>(factory, poolConfig);
        factory.setObjectPool(myPool);
        // 丢弃策略
        AbandonedConfig abandonedConfig = new AbandonedConfig();
        abandonedConfig.setRemoveAbandonedOnMaintenance(true); //在Maintenance的时候检查是否有泄漏
        abandonedConfig.setRemoveAbandonedOnBorrow(true); //borrow 的时候检查泄漏
        abandonedConfig.setRemoveAbandonedTimeout(10); //如果一个对象borrow之后10秒还没有返还给pool，认为是泄漏的对象
        myPool.setAbandonedConfig(abandonedConfig);

        new Thread(()->{
            while (true) {
                try {
                    Thread.currentThread().sleep(1000 * 1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                int numActive = myPool.getNumActive();
                System.err.println("time=" + myPool.getMaxBorrowWaitTimeMillis() + "毫秒,numActive=" + numActive);
            }

        }).start();


        for (int j = 0; j < 30; j++) {
            new Thread(()->{
                int i = 1;
                while (i < 10) {
                    try {
                        Thread.currentThread().sleep(500 * 1);
                        Channel channel = myPool.borrowObject();
                        if (channel.getChannelNumber() >= 10) {
                            System.out.println(channel.getChannelNumber());
                        }
                        String message = "消息-" + i;
                        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
//                        System.out.println(" [x] Sent '" + message + "'");

                        i++;
                        Thread.currentThread().sleep(500 * 1);
                        myPool.returnObject(channel);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            },"publisher_" + j).start();
        }

        System.in.read();
    }

}
