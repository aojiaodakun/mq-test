package com.hzk.mq.tlqmq.producer;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import com.tongtech.tmqi.TopicConnectionFactory;

public class TopicPubNoJNDI {
 
    //==服务节点地址
	public static final String remoteURL = "tlq://172.20.158.201:10024";

	public static void main(String[] args) {
//		String topicName = "ierp_broadcast";
		String topicName = "Event";
        //==连接工厂类
		TopicConnectionFactory testConnFactory = null;
        //==连接类
		Connection myConn = null;
        //==会话类
		Session mySession = null;
        //==打开的主题信息
		Topic testTopic = null;
        //==发布者
		MessageProducer testProducer = null;

		try {
            //==创建连接工厂对象，并设置服务器地址信息，如果应用和TLQ服务端不在同一台机器上，请使用实际的服务端IP和Port替代remoteURL中的127.0.0.1和10024
			testConnFactory = new TopicConnectionFactory();
            testConnFactory.setProperty("tmqiAddressList", remoteURL);
            //==创建Connection
			myConn = testConnFactory.createConnection();
            //==启动连接
			myConn.start();
            //==创建Session
			mySession = myConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            //==设置要打开的TLQ主题信息，以及创建发布者
			testTopic = new com.tongtech.tmqi.Topic(topicName);
			testProducer = mySession.createProducer(testTopic);
            //==生成一个TEXT类型消息
			TextMessage testMessage = mySession	.createTextMessage("TopicPubNoJNDI Message");
		    //==发布消息	
			System.out.println("发送消息...");
			testProducer.send(testMessage);
			System.out.println("发送完成...");

		} catch (Exception jmse) {
			System.out.println("Exception oxxurred :" + jmse.toString());
			jmse.printStackTrace();
		} finally {
			try {
				if (mySession != null) {
                    //==关闭会话
					mySession.close();
				}
				if (myConn != null) {
                    //==关闭连接
					myConn.close();
				}
			} catch (Exception e) {
				System.out.println("退出时发生错误。");
				e.printStackTrace();
			}
		}
	}
}
