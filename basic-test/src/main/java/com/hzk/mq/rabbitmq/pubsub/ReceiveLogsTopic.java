package com.hzk.mq.rabbitmq.pubsub;

import java.io.IOException;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;

public class ReceiveLogsTopic {

	//一个exchange只能是一个类型
	private static final String EXCHANGE_NAME = "topicExchange";
	
	public static void main(String[] args) throws Exception {
		Connection connection = RabbitMQFactory.getConnection();
		Channel channel = connection.createChannel();
		
	    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
	    String queueName = channel.queueDeclare().getQueue();
	    //topic的routingkey，*代表一个具体的单词，#代表0个或多个单词。
	    channel.queueBind(queueName, EXCHANGE_NAME, "*.info");
	    channel.queueBind(queueName, EXCHANGE_NAME, "#.debug");
	    
		Consumer myconsumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope,
					BasicProperties properties, byte[] body)
					throws IOException {
				 System.out.println("========================");
				 String routingKey = envelope.getRoutingKey();
				 System.out.println("routingKey >"+routingKey);
				 String contentType = properties.getContentType();
				 System.out.println("contentType >"+contentType);
				 long deliveryTag = envelope.getDeliveryTag();
				 System.out.println("deliveryTag >"+deliveryTag);
				 System.out.println("content:"+new String(body,"UTF-8"));
				 // (process the message components here ...)
				 //消息处理完后，进行答复。答复过的消息，服务器就不会再次转发。
				 //没有答复过的消息，服务器会一直不停转发。
//				 channel.basicAck(deliveryTag, false);
			}
		};
		channel.basicConsume(queueName,true, myconsumer);
	}
}
