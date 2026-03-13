package com.hzk.mq.rocketmq.v5.producer;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.java.example.ProducerNormalMessageExample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ProducerNormalMessageTest {

    private static final Logger log = LoggerFactory.getLogger(ProducerNormalMessageTest.class);

    public static void main(String[] args) throws ClientException, IOException {
        String topic = "TopicTest";
        String tag = "tagA";
        ClientConfiguration conf = ClientConfiguration.newBuilder()
                .setEndpoints("127.0.0.1:8081") // Proxy 端口
                .build();
        ClientServiceProvider provider = ClientServiceProvider.loadService();

        ProducerBuilder builder = provider.newProducerBuilder()
                .setClientConfiguration(conf)
                .setTopics(topic);
        Producer producer = builder.build();
        // tag过滤
        Message message = provider.newMessageBuilder()
                .setTopic(topic)
                .setTag(tag)
                .setKeys("myKey-01")
                .setBody("Hello gRPC".getBytes())
                .build();
        // SQL消息属性过滤，broker需配置enablePropertyFilter=true
        message = provider.newMessageBuilder()
                .setTopic(topic)
                .addProperty("my_queue", "demo_queue")// 消息属性
                .setKeys("myKey-01")
                .setBody("Hello gRPC".getBytes())
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
