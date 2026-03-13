package com.hzk.mq.tlqmq.consumer;

import com.hzk.mq.tlqmq.factory.TlqMQFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

public class TlqMQTopicConsumerTest {

    private static Connection connection;

    public static void main(String[] args) throws Exception{
        String topicName = "ierp_broadcast";
//        String topicName = "Event";
        connection = TlqMQFactory.getConnection();
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic topic = session.createTopic(topicName);

        MessageConsumer consumer = session.createConsumer(topic);
        consumer.setMessageListener((message) -> {
            try {
                if (message instanceof BytesMessage) {
                    BytesMessage bytesMessage = (BytesMessage)message;
                    byte[] bytes = new byte[(int) ((BytesMessage) message).getBodyLength()];
                    bytesMessage.readBytes(bytes);
                    String body = new String(bytes, "utf-8");
                    System.out.println("received message:" + body);
                    Thread.currentThread().sleep(1000 * 10);
                    message.acknowledge();
                } else {
                    TextMessage textMessage = (TextMessage) message;
                    System.out.println("received message:" + textMessage.getText());
                    message.acknowledge();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });


//        String topicName1 = "State";
//        Topic topic1 = session.createTopic(topicName1);
//        MessageConsumer consumer1 = session.createConsumer(topic1);
//        consumer1.setMessageListener((message) -> {
//            try {
//                if (message instanceof BytesMessage) {
//                    BytesMessage bytesMessage = (BytesMessage)message;
//                    byte[] bytes = new byte[(int) ((BytesMessage) message).getBodyLength()];
//                    bytesMessage.readBytes(bytes);
//                    String body = new String(bytes, "utf-8");
//                    System.out.println("received message:" + body);
//                    Thread.currentThread().sleep(1000);
//                    message.acknowledge();
//                } else {
//                    TextMessage textMessage = (TextMessage) message;
//                    System.out.println("received message:" + textMessage.getText());
//                    message.acknowledge();
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });

    }

}
