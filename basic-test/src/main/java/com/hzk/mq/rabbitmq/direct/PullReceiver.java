package com.hzk.mq.rabbitmq.direct;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;

import java.util.HashMap;
import java.util.Map;

public class PullReceiver {
	private final static String QUEUE_NAME = "hello";
	/**
	 * 主动去服务器上获取消息的消费方式。一般还是会用while(true)保持一个频率长期消费。
	 * @param argv
	 * @throws Exception
	 */
	public static void main(String[] argv) throws Exception {
		Connection connection = RabbitMQFactory.getConnection();
		Channel channel = connection.createChannel();
//		channel.queueDeclare(QUEUE_NAME, false, false, false, null);

		channel.basicQos(100);
		Map<String,Object> params = new HashMap<>();
		//声明Quorum队列的方式就是添加一个x-queue-type参数，指定为quorum。默认是classic
//		params.put("x-queue-type","quorum");
		//队列的声明方式必须保持一致
		channel.queueDeclare(QUEUE_NAME, false, false, false, params);

//		params.put("x-queue-type","stream");
//		params.put("x-max-length-bytes", 20_000_000_000L); // maximum stream size: 20 GB
//		params.put("x-stream-max-segment-size-bytes", 100_000_000); // size of segment files: 100 MB
//		params.put("x-stream-offset", "first");
//		channel.queueDeclare(QUEUE_NAME, false, false, false, params);

	    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
	    GetResponse response = channel.basicGet(QUEUE_NAME, false);
	    if(null != response){
			System.out.println(new String(response.getBody(),"UTF-8"));
		}
		Thread.sleep(10000);
	    GetResponse response2 = channel.basicGet(QUEUE_NAME, false);
	    if(null != response2){
			System.out.println(new String(response2.getBody(),"UTF-8"));
		}

	    channel.close();
	    connection.close();
	}
}
