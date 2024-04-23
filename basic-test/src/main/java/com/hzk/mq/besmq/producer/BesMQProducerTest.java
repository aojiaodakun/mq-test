package com.hzk.mq.besmq.producer;

import com.bes.mq.BESMQConnectionFactory;
import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

public class BesMQProducerTest {

    private BESMQConnectionFactory factory;
    private Connection connection;
    private Session session;
    private MessageProducer producer;
    private String host;
    private String port;
    private String queueName;
    private String username;
    private String password;

    public BesMQProducerTest(String[] args) {
        host = args[0];
        port = args[1];
        username = args[2];
        password = args[3];
        queueName = args[4];
    }

    /**
     * 127.0.0.1 3200 admin admin queueTest
     * @param args
     */
    public static void main(String[] args) {
        BesMQProducerTest sample = new BesMQProducerTest(args);
        sample.execute();
    }

    public void execute() {
        try {
            factory = new BESMQConnectionFactory(username, password, "tcp://" + host + ":" + port);
            connection = factory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(queueName);
            producer = session.createProducer(queue);
            TextMessage message = session.createTextMessage();
            message.setText("HELLO");
            producer.send(message);
            message.setText("GOODBYE");
            producer.send(message);
            message.setText("SHUTDOWN");
            producer.send(message);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (producer != null) {
                try {
                    producer.close();
                } catch (Exception ignore) {
                }
            }
            if (session != null) {
                try {
                    session.close();
                } catch (Exception ignore) {
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception ignore) {
                }
            }
        }
    }

}
