package com.hzk.springboot.config;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.batch.BatchingStrategy;
import org.springframework.amqp.rabbit.batch.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.BatchingRabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitOperations;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.amqp.RabbitTemplateConfigurer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;

@Configuration
public class BatchConfig {

    @Bean
    public Queue batchQueue() {
        return new Queue("batchQueue");
    }

    /**
     * https://docs.spring.io/spring-amqp/reference/amqp/sending-messages.html#template-batching
     * @param connectionFactory
     * @return
     */
    @Bean
    public BatchingRabbitTemplate batchRabbitTemplate(ConnectionFactory connectionFactory) {
        // 创建 BatchingStrategy对象，代表批量策略
        /**
         * 将n条消息的body合并到一条消息
         */
        // 超过收集的消息数量的最大条数
        int batchSize = 5;
        // 每次批量发送消息的最大内存 b
        int bufferLimit = 1024 * 1024;
        // 超过收集的时间的最大等待时长，单位：毫秒
        int timeout = 10 * 1000;
        BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(batchSize, bufferLimit, timeout);
        // 创建 TaskScheduler 对象，用于实现超时发送的定时器
        TaskScheduler taskScheduler = new ConcurrentTaskScheduler();
        // 创建 BatchingRabbitTemplate 对象
        BatchingRabbitTemplate batchTemplate = new BatchingRabbitTemplate(batchingStrategy, taskScheduler);
        batchTemplate.setConnectionFactory(connectionFactory);
        return batchTemplate;
    }

    /**
     * https://docs.spring.io/spring-amqp/reference/amqp/receiving-messages/batch.html
     * @param connectionFactory
     * @return
     */
    @Bean(name="batch")
    public SimpleRabbitListenerContainerFactory consumerBatchContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);

        factory.setMaxConcurrentConsumers(1);
        factory.setConsumerBatchEnabled(true);// 支持批量消费
        factory.setBatchListener(true);
        /**
         * 消费者实际并发度,org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer.createBlockingQueueConsumer
         * int actualPrefetchCount = getPrefetchCount() > this.batchSize ? getPrefetchCount() : this.batchSize;
         */
        factory.setPrefetchCount(10);
        factory.setBatchSize(3);// 批量传入3条消息。实际数量=BatchingStrategy.batchingStrategy*3
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);//手动确认
        return factory;
    }

}
