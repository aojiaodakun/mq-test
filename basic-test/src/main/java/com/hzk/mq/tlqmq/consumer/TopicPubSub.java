package com.hzk.mq.tlqmq.consumer;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import com.tongtech.tmqi.TopicConnectionFactory;

public class TopicPubSub {
    //public static final String tcf = "tongtech.jms.jndi.JmsContextFactory";/* initial context factory*/

	//    public static final String remoteURL = "tlq://127.0.0.1:10024";
	public static final String remoteURL = "tlq://172.20.158.201:10024";
    //public static final String remoteFactory = "RemoteConnectionFactory";

	public static void main(String[] args) {
		String topic = "State";

		TopicConnectionFactory testConnFactory = null;
		Connection myConn = null;
		Session mySession = null;
		Topic testTopic = null;
		MessageProducer testProducer = null;
		MessageConsumer testConsumer = null;
		MessageConsumer anotherTestConsumer = null;
		TextMessage testMessage = null;

		try {
			testConnFactory = new TopicConnectionFactory();
            testConnFactory.setProperty("tmqiAddressList", remoteURL);
//			testConnFactory.setProperty("tmqiDefaultUsername", "guest");
//			testConnFactory.setProperty("tmqiDefaultPassword", "guest");
			testTopic = new com.tongtech.tmqi.Topic("Event");
			myConn = testConnFactory.createConnection();
			mySession = myConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			testProducer = mySession.createProducer(testTopic);
			testConsumer = mySession.createConsumer(testTopic);
			anotherTestConsumer = mySession.createConsumer(testTopic);
			testMessage = mySession.createTextMessage("TopicPubSub Message");
			myConn.start();
			
			System.out.println("send message");
			testProducer.send(testMessage);

			Topic testTopic1 = new com.tongtech.tmqi.Topic(topic);
			MessageProducer testProducer1 = mySession.createProducer(testTopic1);
			TextMessage testMessage1 = mySession.createTextMessage("state...");
			testProducer1.send(testMessage1);

			System.out.println("pull message");
			Message msg = testConsumer.receive(1000);
			if(msg != null){
				if(msg instanceof TextMessage){
					TextMessage message = (TextMessage)msg;
					System.out.println("testConsumer:"+message.getText());
				}else{
					System.out.println("testConsumer.");
				}
			}
			msg = anotherTestConsumer.receive(1000);
			if(msg != null){
				if(msg instanceof TextMessage){
					TextMessage message = (TextMessage)msg;
					System.out.println("anotherTestConsumer:"+message.getText());
				}else{
					System.out.println("anotherTestConsumer...");
				}
			}else{
				System.out.println("receive nothing");
			}

		} catch (Exception jmse) {
			System.out.println("Exception oxxurred :" + jmse.toString());
			jmse.printStackTrace();
		} finally {
			try {
				if (mySession != null) {
					mySession.close();
				}
				if (myConn != null) {
					myConn.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}


}
