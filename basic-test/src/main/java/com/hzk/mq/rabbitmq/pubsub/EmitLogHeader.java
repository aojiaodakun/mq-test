package com.hzk.mq.rabbitmq.pubsub;

import java.util.HashMap;
import java.util.Map;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

public class EmitLogHeader {

	private static final String EXCHANGE_NAME = "logs";
	/**
	 * exchange有四种类型， fanout topic headers direct
	 * headers用得比较少，他是根据头信息来判断转发路由规则。头信息可以理解为一个Map
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{
		// header模式不需要routingKey来转发，他是根据header里的信息来转发的。比如消费者可以只订阅logLevel=info的消息。
		// 然而，消息发送的API还是需要一个routingKey。 
		// 如果使用header模式来转发消息，routingKey可以用来存放其他的业务消息，客户端接收时依然能接收到这个routingKey消息。
		String routingKey = "ourTestRoutingKey";
		// The map for the headers.
		Map<String, Object> headers = new HashMap<>();
		headers.put("loglevel", "info");
		headers.put("buslevel", "product");
		headers.put("syslevel", "admin");
		
		String message = "LOG INFO asdfasdf";

		Connection connection = RabbitMQFactory.getConnection();
		Channel channel = connection.createChannel();
		//发送者只管往exchange里发消息，而不用关心具体发到哪些queue里。
		channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.HEADERS);
		
		AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder(); 
		builder.deliveryMode(MessageProperties.PERSISTENT_TEXT_PLAIN.getDeliveryMode());
		builder.priority(MessageProperties.PERSISTENT_TEXT_PLAIN.getPriority());
		builder.headers(headers);

		channel.basicPublish(EXCHANGE_NAME, routingKey, builder.build(), message.getBytes("UTF-8"));

//		channel.txSelect();
//		channel.txCommit();
//		channel.txRollback();

		channel.close();
		connection.close();
	}
}
