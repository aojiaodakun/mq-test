package com.hzk.mq.tlqmq.consumer;

import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;

import com.tongtech.tmqi.TopicConnectionFactory;

import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.Context;
import javax.naming.InitialContext;

import com.tongtech.jms.FileMessage;

/**
 * 重启一次，TLQ.SYS.BROKER.SUB的数量+1
 */
public class TopicConsumer {

    // public static final String tcf = "tongtech.jms.jndi.JmsContextFactory";/* initial context factory*/

//    public static final String remoteURL = "tlq://127.0.0.1:10024";
    public static final String remoteURL = "tlq://172.20.158.201:10024";
    //public static final String remoteFactory = "RemoteConnectionFactory";


    public static void main(String[] args) {
        String topicName = "ierp_broadcast";
//        String topicName = "Event";

        TopicConnectionFactory topicCF = null;
        TopicConnection conn = null;
        TopicSession session = null;
        Topic topic = null;
        try {
//            java.util.Properties prop = new java.util.Properties();
//
//            prop.put("java.naming.factory.initial", tcf);
//            prop.put("java.naming.provider.url", remoteURL);
//            Context ctx = new InitialContext(prop);

            topicCF = new TopicConnectionFactory();
            topicCF.setProperty("tmqiAddressList", remoteURL);
//            topicCF.setProperty("tmqiDefaultUsername", "guest");
//            topicCF.setProperty("tmqiDefaultPassword", "guest");
            topic = new com.tongtech.tmqi.Topic(topicName);
//            topicCF = (TopicConnectionFactory) ctx.lookup(remoteFactory);
            /**
             * 持久订阅修改这两个，订阅端退出后，再启动还会有消息
             * 持久化订阅条件1，
             */
//            topic = (Topic) ctx.lookup("yiyang");
            conn = topicCF.createTopicConnection();
//            conn.setClientID("hzk-1");
            conn.start();


            session = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

//            log.info(String.format("Begin to consume topic[%s]",tlqConfig.getConsumeTopic()));
            TopicSubscriber subscriber = session.createSubscriber(topic);
            // 持久化订阅条件2
//            TopicSubscriber subscriber = session.createDurableSubscriber(topic, "111");
            MessageRecTopic receiver = new MessageRecTopic();
            subscriber.setMessageListener(receiver);

        } catch (Exception e) {
            System.out.println("Exception oxxurred :" + e.toString());
        }
    }
}

class MessageRecTopic implements javax.jms.MessageListener {

    public void onMessage(Message message) {
        try {
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                System.out.println("收到一条Text消息:" + textMessage.getText());
            } else if (message instanceof MapMessage) {
                System.out.println("收到一条Map消息");
            } else if (message instanceof StreamMessage) {
                System.out.println("收到一条Text消息");
            } else if (message instanceof BytesMessage) {
                System.out.println("收到一条Bytes消息");
            } else if (message instanceof ObjectMessage) {
                System.out.println("收到一条Object消息");
            } else if (message instanceof FileMessage) {
                System.out.println("收到一条File消息");
            }
            synchronized (this) {
                notify();
            }
        } catch (Exception jmse) {
            System.out.println("Exception oxxurred :" + jmse.toString());
            jmse.printStackTrace();
        }

    }
}
