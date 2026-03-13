package com.hzk.mq.rabbitmq.stream;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author roy
 * @date 2022/4/25
 * @desc
 */
public class StreamPushConsumer {
    private static final String QUEUE_NAME = "streamQueue";

    /**
     * 保持长连接，等待服务器推送的消费方式。
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Connection connection = RabbitMQFactory.getConnection();
        Channel channel = connection.createChannel();
        //1、这个属性必须设置。
        channel.basicQos(1);
        //2、声明Stream队列
        Map<String,Object> params = new HashMap<>();
        params.put("x-queue-type","stream");
        params.put("x-max-length-bytes", 20_000_000_000L); // maximum stream size: 20 GB
        params.put("x-stream-max-segment-size-bytes", 100_000_000); // size of segment files: 100 MB
        channel.queueDeclare(QUEUE_NAME, true, false, false, params);

        //Consumer接口还一个实现QueueConsuemr 但是代码注释过期了。
        Consumer myconsumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                System.out.println("========================");
                String routingKey = envelope.getRoutingKey();
                System.out.println("routingKey >" + routingKey);
                String contentType = properties.getContentType();
                System.out.println("contentType >" + contentType);
                long deliveryTag = envelope.getDeliveryTag();
                System.out.println("deliveryTag >" + deliveryTag);
                System.out.println("content:" + new String(body, "UTF-8"));
                // (process the message components here ...)
                //消息处理完后，进行答复。答复过的消息，服务器就不会再次转发。
                //没有答复过的消息，服务器会一直不停转发。
                channel.basicAck(deliveryTag, false);
            }
        };
        //3、消费时，必须指定offset。 可选的值：
        // first: 从日志队列中第一个可消费的消息开始消费
        // last: 消费消息日志中最后一个消息
        // next: 相当于不指定offset，消费不到消息。
        // Offset: 一个数字型的偏移量
        // Timestamp:一个代表时间的Data类型变量，表示从这个时间点开始消费。例如 一个小时前 Date timestamp = new Date(System.currentTimeMillis() - 60 * 60 * 1_000)
        Map<String,Object> consumeParam = new HashMap<>();
        consumeParam.put("x-stream-offset","last");
        channel.basicConsume(QUEUE_NAME, false,consumeParam, myconsumer);

//        channel.close();
    }
}
