package com.hzk.mq.rabbitmq.task;

import java.io.IOException;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * 可以启动多个worker，同时等待NewTask发送一个消息，task将由其中一个worker完成。
 * @author Administrator
 *
 */
public class Worker {

	public static void main(String[] args) throws Exception{
		Connection connection = RabbitMQFactory.getConnection();
		Channel channel = connection.createChannel();
		//这个任务场景一般任务不能因为rabbitmq崩溃而消失，所以把第二个是否持久化设置成true。
		//这样，即使rabbitmq服务重启，任务不会丢失
		channel.queueDeclare("work", true, false, false, null);
		//每个worker同时最多只处理一个消息
		channel.basicQos(1);
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
				 channel.basicAck(deliveryTag, false);
				}
			};
		channel.basicConsume("work", myconsumer);
	}
}
