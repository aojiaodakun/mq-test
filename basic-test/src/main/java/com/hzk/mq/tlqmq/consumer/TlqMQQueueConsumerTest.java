package com.hzk.mq.tlqmq.consumer;

import com.hzk.mq.tlqmq.factory.TlqMQFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

public class TlqMQQueueConsumerTest {

    public static void main(String[] args) throws Exception{
        String queueName = "test_queue";// UsageType = 0		# 队列的使用类型，0：普通本地队列，1：发布订阅队列，2：死信队列
        Connection connection = TlqMQFactory.getConnection();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(queueName);

        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener((message) -> {
            try {
                if (message instanceof BytesMessage) {
                    BytesMessage bytesMessage = (BytesMessage)message;
                    byte[] bytes = new byte[(int) ((BytesMessage) message).getBodyLength()];
                    bytesMessage.readBytes(bytes);
                    String body = new String(bytes, "utf-8");
                    System.out.println("received message:" + body);
                    Thread.currentThread().sleep(1000);
                    message.acknowledge();
                } else {
                    message.acknowledge();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

}
