package com.hzk.mq.besmq.consumer;

import com.bes.mq.BESMQConnectionFactory;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

public class BesMQConsumerTest {

    private BESMQConnectionFactory factory;
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private String host;
    private String port;
    private String queueName;
    private String username;
    private String password;

    public BesMQConsumerTest(String[] args) {
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
        BesMQConsumerTest sample = new BesMQConsumerTest(args);
        sample.execute();
    }

    public void execute() {
        try {
            factory = new BESMQConnectionFactory(username, password, "tcp://" + host + ":" + port);
            connection = factory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(queueName);
            consumer = session.createConsumer(queue);

            new Thread(()->{
                try {
                    MessageConsumer consumer = session.createConsumer(queue);
                    while (true) {
                        TextMessage message = (TextMessage) consumer.receive(60 * 1000);
                        if (message != null) {
                            System.out.println(Thread.currentThread().getName() + ",Received message : " + message.getText());
                        }
                    }
                } catch (Exception e) {

                }
            }, "hzkConsumer").start();

            int count = 0;
            System.out.println("Consumer is waiting to receive the message...");
            while (true) {
                TextMessage message = (TextMessage) consumer.receive(60 * 1000);
                if (message != null) {
                    System.out.println("Received " + ++count + " message" + (count > 1 ? "s." : "."));
                    System.out.println("Received message : " + message.getText());
//                if (message.getText().equals("SHUTDOWN")) {
//                    break;
//                }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (consumer != null) {
                try {
                    consumer.close();
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
