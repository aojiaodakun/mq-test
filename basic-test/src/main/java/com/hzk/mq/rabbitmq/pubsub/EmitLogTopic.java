package com.hzk.mq.rabbitmq.pubsub;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class EmitLogTopic {

	private static final String EXCHANGE_NAME = "topicExchange";
	/**
	 * exchange有四种类型， fanout topic headers direct
	 * topic类型的exchange在根据routingkey转发消息时，可以对rouytingkey做一定的规则，比如anonymous.info可以被*.info匹配到。
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{
		Connection connection = RabbitMQFactory.getConnection();
		Channel channel = connection.createChannel();
		//发送者只管往exchange里发消息，而不用关心具体发到哪些queue里。
		channel.exchangeDeclare(EXCHANGE_NAME, "topic");
		String message = "LOG INFO";
		channel.basicPublish(EXCHANGE_NAME, "anonymous.info", null, message.getBytes());
		channel.basicPublish(EXCHANGE_NAME, "tuling.loulan.debug", null, message.getBytes());

		channel.close();
		connection.close();
	}
}
