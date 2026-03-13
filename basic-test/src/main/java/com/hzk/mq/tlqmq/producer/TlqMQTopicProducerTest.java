package com.hzk.mq.tlqmq.producer;

import com.hzk.mq.tlqmq.factory.TlqMQFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

public class TlqMQTopicProducerTest {

    public static void main(String[] args) throws Exception{
        String topicName = "ierp_broadcast";
//        String topicName = "Event";
        String body = "hello";

        Connection connection = TlqMQFactory.getConnection();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic topic = session.createTopic(topicName);

        MessageProducer producer = session.createProducer(topic);
//        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);// 非持久化
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);// 持久化
        // 发消息
        for (int i = 0; i < 10; i++) {
            BytesMessage bytesMessage = session.createBytesMessage();
            bytesMessage.setStringProperty("tag", topicName);
            bytesMessage.writeBytes((body+i).getBytes("utf-8"));
            producer.send(bytesMessage);
        }

        // 关闭资源
        producer.close();
        session.close();
        connection.close();
    }

}
