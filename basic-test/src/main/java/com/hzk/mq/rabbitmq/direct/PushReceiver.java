package com.hzk.mq.rabbitmq.direct;

import java.io.IOException;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class PushReceiver {

	private static final String QUEUE_NAME = "hello";
	/**
	 * 保持长连接，等待服务器推送的消费方式。
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Connection connection = RabbitMQFactory.getConnection();
		Channel channel = connection.createChannel();
		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		//Consumer接口还一个实现QueueConsuemr 但是代码注释过期了。
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
				System.out.println("messageId:"+properties.getMessageId());
				properties.getHeaders().forEach((key,value)-> System.out.println("key: "+key +"; value: "+value));
				// (process the message components here ...)
				 //消息处理完后，进行答复。答复过的消息，服务器就不会再次转发。
				 //没有答复过的消息，服务器会一直不停转发。
				 channel.basicAck(deliveryTag, false);
			}
		};
		
		channel.basicConsume(QUEUE_NAME, false, myconsumer);
	}
}
