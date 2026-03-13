package com.hzk.mq.rabbitmq.pubsub;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class ReceiveLogsHeader {

	private static final String EXCHANGE_NAME = "logs";
	
	public static void main(String[] args) throws Exception {
		
		String routingKey= "ourTestRoutingKey";
		
		Map<String, Object> headers = new HashMap<String, Object>();
//		headers.put("loglevel", "info");
		headers.put("buslevel", "product");
//		headers.put("syslevel", "admin");

		Connection connection = RabbitMQFactory.getConnection();
		Channel channel = connection.createChannel();
		
	    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.HEADERS);
	    String queueName = channel.queueDeclare("ReceiverHeader",true,false,false,null).getQueue();
	    
	    channel.queueBind(queueName, EXCHANGE_NAME,routingKey,headers);
	    
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
				Map<String, Object> headerInfo = properties.getHeaders();
				headerInfo.forEach((key,value)-> System.out.println("header key: "+key+"; value: "+value));
				System.out.println("content:"+new String(body,"UTF-8"));
				 // (process the message components here ...)
				 //消息处理完后，进行答复。答复过的消息，服务器就不会再次转发。
				 //没有答复过的消息，服务器会一直不停转发。
				 channel.basicAck(deliveryTag, false);
			}
		};
		
		String consumerTag = channel.basicConsume(queueName,true, myconsumer);
		System.out.println("consumerTag > "+consumerTag);
	}
}
