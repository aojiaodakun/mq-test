package com.hzk.mq.rabbitmq.task;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

public class NewTask {

	/**
	 * 发布一个task，交由多个Worker去处理。 每个task只要由一个Worker完成就行。
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Connection connection = RabbitMQFactory.getConnection();
		Channel channel = connection.createChannel();
		channel.queueDeclare("work", true, false, false, null);
		String message = "task 1";
		channel.basicPublish("", "work",
				MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());

		channel.close();
		connection.close();
	}
}
