package com.hzk.mq.rabbitmq.pubsub;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class EmitLogDirect {

	private static final String EXCHANGE_NAME = "directExchange";
	/**
	 * exchange有四种类型， fanout topic headers direct
	 * direct类型的exchange会根据routingkey，将消息转发到该exchange上绑定了该routingkey的所有queue
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{
		Connection connection = RabbitMQFactory.getConnection();
		Channel channel = connection.createChannel();
		//发送者只管往exchange里发消息，而不用关心具体发到哪些queue里。
		channel.exchangeDeclare(EXCHANGE_NAME, "direct");
		String message = "LOG INFO 44444";
//		channel.basicPublish(EXCHANGE_NAME, "info", null, message.getBytes());
//		channel.basicPublish(EXCHANGE_NAME, "debug", null, message.getBytes());
		channel.basicPublish(EXCHANGE_NAME, "warn", null, message.getBytes());

		channel.close();
		connection.close();
		
		
	}
}
