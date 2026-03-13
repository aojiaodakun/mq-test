package com.hzk.mq.rocketmq.v5.producer;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 顺序消息
 * 在 RocketMQ 4.x（Remoting 协议）中：
 * 顺序消息通过 MessageQueue 的单线程消费 + 同一个 queueId 的绑定 hash key 来保证顺序。
 *
 * 在 RocketMQ 5.x （gRPC + Proxy 架构）中：
 * 不再暴露 queueId 概念，改为使用 MessageGroup 来实现顺序保证。
 */
public class ProducerFifoMessageTest {

    private static final Logger log = LoggerFactory.getLogger(ProducerFifoMessageTest.class);

    public static void main(String[] args) throws ClientException, IOException {
        String topic = "TopicTest";
        String tag = "tagA";
        String consumerGroup = "myTestGroup";
        ClientConfiguration conf = ClientConfiguration.newBuilder()
                .setEndpoints("127.0.0.1:8081") // Proxy 端口
                .build();
        ClientServiceProvider provider = ClientServiceProvider.loadService();

        ProducerBuilder builder = provider.newProducerBuilder()
                .setClientConfiguration(conf)
                .setTopics(topic);
        Producer producer = builder.build();
        Message message = provider.newMessageBuilder()
                .setTopic(topic)
                .setTag(tag)
                .setKeys("myKey-01")
                .setBody("Hello gRPC".getBytes())
                .setMessageGroup(consumerGroup)// 指定consumerGroup实现FIFO
                .build();
        try {
            SendReceipt sendReceipt = producer.send(message);
            log.info("Send message successfully, messageId={}", sendReceipt.getMessageId());
        } catch (Throwable t) {
            log.error("Failed to send message", t);
        }
        producer.close();
    }

}
