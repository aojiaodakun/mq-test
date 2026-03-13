package com.hzk.mq.rocketmq.v5.consumer;

import com.hzk.mq.rocketmq.v5.producer.ProducerNormalMessageTest;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class SimpleConsumerTest {

    private static final Logger log = LoggerFactory.getLogger(SimpleConsumerTest.class);

    public static void main(String[] args) throws Exception {
        String topic = "TopicTest";
        String tag = "tagA";
        String consumerGroup = "myTestGroup";
        ClientConfiguration conf = ClientConfiguration.newBuilder()
                .setEndpoints("127.0.0.1:8081") // Proxy 端口
                .build();
        ClientServiceProvider provider = ClientServiceProvider.loadService();


        Duration awaitDuration = Duration.ofSeconds(30);
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        SimpleConsumer consumer = provider.newSimpleConsumerBuilder()
                .setClientConfiguration(conf)
                // Set the consumer group name.
                .setConsumerGroup(consumerGroup)
                // set await duration for long-polling.
                .setAwaitDuration(awaitDuration)
                // Set the subscription for the consumer.
                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                .build();
        // Max message num for each long polling.
        int maxMessageNum = 16;
        // Set message invisible duration after it is received.
        Duration invisibleDuration = Duration.ofSeconds(15);
        // Receive message, multi-threading is more recommended.
        do {
            final List<MessageView> messages = consumer.receive(maxMessageNum, invisibleDuration);
            log.info("Received {} message(s)", messages.size());
            for (MessageView message : messages) {
                final MessageId messageId = message.getMessageId();
                try {
                    consumer.ack(message);
                    log.info("Message is acknowledged successfully, messageId={}", messageId);
                } catch (Throwable t) {
                    log.error("Message is failed to be acknowledged, messageId={}", messageId, t);
                }
            }
        } while (true);
    }



}
