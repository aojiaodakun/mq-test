package com.hzk.mq.rocketmq.v5.consumer;

import com.hzk.mq.rocketmq.v5.producer.ProducerNormalMessageTest;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class PushConsumerTest {

    private static final Logger log = LoggerFactory.getLogger(PushConsumerTest.class);

    public static void main(String[] args) throws Exception {
        String topic = "TopicTest";
        String tag = "tagA";
        String consumerGroup = "myTestGroup";
        ClientConfiguration conf = ClientConfiguration.newBuilder()
                .setEndpoints("127.0.0.1:8081") // Proxy 端口
                .build();
        ClientServiceProvider provider = ClientServiceProvider.loadService();

        // tag过滤
//        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
//         SQL消息属性过滤，broker需配置enablePropertyFilter=true
        FilterExpression filterExpression = new FilterExpression("my_queue='demo_queue'", FilterExpressionType.SQL92);
        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(conf)
                .setConsumerGroup(consumerGroup)
                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                .setMessageListener(new MyTestMessageListener())
                .build();

        Thread.sleep(Long.MAX_VALUE);
        pushConsumer.close();
    }

    private static class MyTestMessageListener implements MessageListener {

        @Override
        public ConsumeResult consume(MessageView messageView) {
            log.info("Consume message={}", messageView);
            return ConsumeResult.SUCCESS;
        }
    }

}
