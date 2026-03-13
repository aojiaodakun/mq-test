package com.hzk.mq.rabbitmq.factory;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;


/**
 * 为什么会有 RESOURCE_LOCKED？
 * RabbitMQ 对独占队列的处理：
 * 1、连接断开时，不会立即删除独占队列
 * 2、有一个清理延迟，在此期间队列处于"锁定"状态
 * 3、其他连接尝试声明同名独占队列会被拒绝
 *
 * 2026-02-06 01:05:33.357 [error] <0.6357.213> Channel error on connection <0.6293.213> (10.14.9.192:49803 -> 10.14.9.189:5672, vhost: 'ierp', user: 'cosmic'), channel 2:
 * operation queue.declare caused a channel exception resource_locked: cannot obtain exclusive access to locked queue 'broadcast_mservice-qing-3197242922' in vhost 'ierp'. It could be originally declared on another connection or the exclusive property value does not match that of the original declaration.
 */
public class RobustQueueManager {

    private static final Logger logger = LoggerFactory.getLogger(RobustQueueManager.class);
    private final ConnectionFactory connectionFactory;
    private final Map<String, ReentrantLock> queueLocks = new ConcurrentHashMap<>();

    public RobustQueueManager(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /**
     * 安全的队列声明方法
     */
    public boolean safeDeclareQueue(QueueConfig config) {
        String queueName = config.getQueueName();

        // 获取队列级别的锁，防止并发声明
        ReentrantLock lock = queueLocks.computeIfAbsent(queueName, k -> new ReentrantLock());

        if (!lock.tryLock()) {
            logger.debug("队列 {} 正在被其他线程声明，等待...", queueName);
            try {
                if (lock.tryLock(30, TimeUnit.SECONDS)) {
                    return doDeclareQueue(config);
                } else {
                    logger.error("获取队列 {} 声明锁超时", queueName);
                    return false;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("获取队列 {} 声明锁被中断", queueName, e);
                return false;
            } finally {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        } else {
            try {
                return doDeclareQueue(config);
            } finally {
                lock.unlock();
            }
        }
    }

    private boolean doDeclareQueue(QueueConfig config) {
        Connection connection = null;
        Channel channel = null;

        try {
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();

            String queueName = config.getQueueName();

            // 检查队列是否已存在
            QueueInfo existingQueue = checkQueueExists(channel, queueName);

            if (existingQueue != null) {
                // 队列已存在，检查属性是否匹配
                if (isQueuePropertiesMatch(existingQueue, config)) {
                    logger.info("队列 {} 已存在且属性匹配，跳过声明", queueName);
                    return true;
                } else {
                    logger.warn("队列 {} 已存在但属性不匹配，尝试重新声明", queueName);
                    // 根据业务需求决定：删除重建或使用现有队列
                    return handleMismatchQueue(channel, existingQueue, config);
                }
            } else {
                // 队列不存在，正常声明
                return declareNewQueue(channel, config);
            }

        } catch (Exception e) {
            logger.error("处理队列 {} 声明失败", config.getQueueName(), e);
            return false;
        } finally {
            closeResources(channel, connection);
        }
    }

    // TODO
    private boolean handleMismatchQueue(Channel channel, QueueInfo existingQueue, QueueConfig config) {
        return false;
    }

    /**
     * 检查队列是否存在
     */
    private QueueInfo checkQueueExists(Channel channel, String queueName) {
        try {
            AMQP.Queue.DeclareOk declareOk = channel.queueDeclarePassive(queueName);

            return QueueInfo.builder()
                    .queueName(queueName)
                    .messageCount(declareOk.getMessageCount())
                    .consumerCount(declareOk.getConsumerCount())
                    .exists(true)
                    .build();

        } catch (IOException e) {
            if (e.getMessage().contains("404")) {
                // 队列不存在
                return null;
            } else {
                logger.warn("检查队列 {} 存在性时发生异常", queueName, e);
                return null;
            }
        }
    }

    /**
     * 声明新队列
     */
    private boolean declareNewQueue(Channel channel, QueueConfig config) throws IOException {
        String queueName = config.getQueueName();

        try {
            AMQP.Queue.DeclareOk declareOk = channel.queueDeclare(
                    queueName,
                    config.isDurable(),
                    config.isExclusive(),
                    config.isAutoDelete(),
                    config.getArguments()
            );

            logger.info("成功声明队列: {} (消息数: {}, 消费者数: {})",
                    queueName, declareOk.getMessageCount(), declareOk.getConsumerCount());
            return true;

        } catch (AlreadyClosedException e) {
            if (e.getReason() != null && e.getReason().protocolMethodName().equals("channel.close")) {
                ShutdownSignalException sse = (ShutdownSignalException) e.getCause();
                if (sse.getMessage().contains("RESOURCE_LOCKED")) {
                    logger.warn("队列 {} 被锁定，处理锁定场景", queueName);
                    return handleLockedQueue(channel, config);
                }
            }
            throw e;
        }
    }

    /**
     * 处理被锁定的队列
     */
    private boolean handleLockedQueue(Channel channel, QueueConfig config) {
        // 方案1: 如果是独占队列，等待原始连接释放
        if (config.isExclusive()) {
            logger.info("队列 {} 是独占队列，等待原始连接释放...", config.getQueueName());
            try {
                Thread.sleep(5000); // 等待5秒
                return declareNewQueue(channel, config);
            } catch (Exception e) {
                logger.error("重试声明独占队列 {} 失败", config.getQueueName(), e);
            }
        }

        // 方案2: 使用不同的队列名
        String alternativeName = config.getQueueName() + "_" + System.currentTimeMillis();
        logger.info("为队列 {} 使用替代名称: {}", config.getQueueName(), alternativeName);

        try {
            AMQP.Queue.DeclareOk declareOk = channel.queueDeclare(
                    alternativeName,
                    config.isDurable(),
                    false, // 使用非独占模式
                    config.isAutoDelete(),
                    config.getArguments()
            );
            // 通知业务层使用新的队列名
            config.onAlternativeQueueCreated(alternativeName);
            return true;
        } catch (IOException e) {
            logger.error("声明替代队列失败", e);
            return false;
        }
    }

    /**
     * 检查队列属性是否匹配
     */
    private boolean isQueuePropertiesMatch(QueueInfo existing, QueueConfig config) {
        // 这里可以根据需要实现具体的属性匹配逻辑
        // 例如检查 durable、exclusive 等属性是否一致
        return true; // 简化实现
    }

    private void closeResources(Channel channel, Connection connection) {
        if (channel != null && channel.isOpen()) {
            try {
                channel.close();
            } catch (Exception e) {
                logger.warn("关闭 Channel 时发生异常", e);
            }
        }

        if (connection != null && connection.isOpen()) {
            try {
                connection.close();
            } catch (Exception e) {
                logger.warn("关闭 Connection 时发生异常", e);
            }
        }
    }
}

/**
 * 队列配置类
 */
class QueueConfig {
    private String queueName;
    private boolean durable = true;
    private boolean exclusive = false;
    private boolean autoDelete = false;
    private Map<String, Object> arguments;
    private QueueDeclareCallback callback;

    // getters and setters
    public String getQueueName() { return queueName; }
    public boolean isDurable() { return durable; }
    public boolean isExclusive() { return exclusive; }
    public boolean isAutoDelete() { return autoDelete; }
    public Map<String, Object> getArguments() { return arguments; }

    public void onAlternativeQueueCreated(String alternativeName) {
        if (callback != null) {
            callback.onAlternativeQueue(alternativeName);
        }
    }

    // Builder 模式
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private QueueConfig config = new QueueConfig();

        public Builder queueName(String queueName) {
            config.queueName = queueName;
            return this;
        }

        public Builder durable(boolean durable) {
            config.durable = durable;
            return this;
        }

        public Builder exclusive(boolean exclusive) {
            config.exclusive = exclusive;
            return this;
        }

        public Builder autoDelete(boolean autoDelete) {
            config.autoDelete = autoDelete;
            return this;
        }

        public Builder arguments(Map<String, Object> arguments) {
            config.arguments = arguments;
            return this;
        }

        public Builder callback(QueueDeclareCallback callback) {
            config.callback = callback;
            return this;
        }

        public QueueConfig build() {
            return config;
        }
    }
}

/**
 * 回调接口
 */
interface QueueDeclareCallback {
    void onAlternativeQueue(String alternativeName);
}

/**
 * 队列信息类
 */
class QueueInfo {
    private String queueName;
    private int messageCount;
    private int consumerCount;
    private boolean exists;

    // Builder 模式
    public static Builder builder() {
        return new Builder();
    }

    // getters and builder
    static class Builder {
        private QueueInfo info = new QueueInfo();

        Builder queueName(String queueName) {
            info.queueName = queueName;
            return this;
        }

        Builder messageCount(int count) {
            info.messageCount = count;
            return this;
        }

        Builder consumerCount(int count) {
            info.consumerCount = count;
            return this;
        }

        Builder exists(boolean exists) {
            info.exists = exists;
            return this;
        }

        QueueInfo build() {
            return info;
        }
    }
}
