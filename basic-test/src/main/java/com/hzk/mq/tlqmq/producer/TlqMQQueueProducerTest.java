package com.hzk.mq.tlqmq.producer;

import com.hzk.mq.tlqmq.factory.TlqMQFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

public class TlqMQQueueProducerTest {

    public static void main(String[] args) throws Exception{
        String queueName = "test_queue";
        String body = "hello";

        Connection connection = TlqMQFactory.getConnection();
        boolean autoAck = false;
        Session session = connection.createSession(false, autoAck ? Session.AUTO_ACKNOWLEDGE : Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(queueName);

        MessageProducer producer = session.createProducer(queue);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);// 非持久化
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);// 持久化
        // 发消息
        for (int i = 0; i < 1; i++) {
            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.writeBytes((body+i).getBytes("utf-8"));
            producer.send(bytesMessage);
        }

        // 关闭资源
        producer.close();
        session.close();
        connection.close();
    }

}
