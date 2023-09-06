package com.hzk.mq.rabbitmq.producer;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;

import java.time.Duration;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BooleanSupplier;

/**
 * Publisher Confirms(消息发送确认),发送消息的时候对发送的消息进行确认,对发送失败的消息可以进行进一步的处理
 */
public class RabbitMQPublisherConfirmsProducer {

    private final static String QUEUE_NAME = "publisher_confirms";
    private static final int MESSAGE_COUNT = 50_000;

    public static void main(String[] args) throws Exception{
        // 单条消息确认,速度较慢,吞吐量不高
        publishMessagesIndividually();
        // 批量确认,速度较快,吞吐量较大;但如果确认失败不知道那条消息出问题了
        publishMessagesInBatch();
        // 异步确认,最佳性能和资源使用,但编码有些复杂
        handlePublishConfirmsAsynchronously();
    }

    /**
     * 单条消息确认,速度较慢,吞吐量不高
     * @throws Exception
     */
    private static void publishMessagesIndividually() throws Exception{
        Connection connection = null;
        Channel channel = null;
        try {
            connection = RabbitMQFactory.getConnection();
            channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, true, null);
            //开启消息确认，默认时关闭的
            channel.confirmSelect();
            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                channel.basicPublish("", QUEUE_NAME, null, body.getBytes());
                //超时或发送失败抛出异常
                channel.waitForConfirmsOrDie(5_000);
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages individually in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            channel.close();
            connection.close();
        }
    }

    /**
     * 批量确认,速度较快,吞吐量较大;但如果确认失败不知道那条消息出问题了
     * @throws Exception
     */
    private static void publishMessagesInBatch() throws Exception{
        Connection connection = null;
        Channel channel = null;
        try {
            connection = RabbitMQFactory.getConnection();
            channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, true, null);
            //开启消息确认，默认时关闭的
            channel.confirmSelect();

            int batchSize = 100;
            int outstandingMessageCount = 0;

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                channel.basicPublish("", QUEUE_NAME, null, body.getBytes());
                outstandingMessageCount++;

                if (outstandingMessageCount == batchSize) {
                    channel.waitForConfirmsOrDie(5_000);
                    outstandingMessageCount = 0;
                }
            }

            if (outstandingMessageCount > 0) {
                channel.waitForConfirmsOrDie(5_000);
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages in batch in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            channel.close();
            connection.close();
        }
    }

    /**
     * 异步确认,最佳性能和资源使用,但编码有些复杂
     */
    private static void handlePublishConfirmsAsynchronously() throws Exception{
        Connection connection = null;
        Channel channel = null;
        try {
            connection = RabbitMQFactory.getConnection();
            channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, true, null);
            //开启消息确认，默认时关闭的
            channel.confirmSelect();

            ConcurrentNavigableMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();

            ConfirmCallback ackCallback = (sequenceNumber, multiple) -> {
                if (multiple) {
                    ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(sequenceNumber,true);
                    confirmed.clear();
                } else {
                    outstandingConfirms.remove(sequenceNumber);
                }
            };
            ConfirmCallback nackCallback = (sequenceNumber, multiple) -> {
                String body = outstandingConfirms.get(sequenceNumber);
                System.err.format("Message with body %s has been nack-ed. Sequence number: %d, multiple: %b%n", body, sequenceNumber, multiple);

                /*
                 *消息发送失败时这边再次调用发送成功的处理方法，也可以把失败的消息(获取失败消息的方法同ackCallback里方法)重新发送，不过不能在这里发送消息(rabbitmq不支持)，
                 *可以把失败的消息发送到ConcurrentLinkedQueue,发送消息的线程从该ConcurrentLinkedQueue取数据来发送消息
                 */
                ackCallback.handle(sequenceNumber, multiple);
            };
            channel.addConfirmListener(ackCallback, nackCallback);

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                outstandingConfirms.put(channel.getNextPublishSeqNo(), body);
                channel.basicPublish("", QUEUE_NAME, null, body.getBytes());
            }

            if (!waitUntil(Duration.ofSeconds(60), () -> outstandingConfirms.isEmpty())) {
                throw new IllegalStateException("All messages could not be confirmed in 60 seconds");
            }

            long end = System.nanoTime();
            System.out.format("Published %,d messages and handled confirms asynchronously in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            channel.close();
            connection.close();
        }
    }

    private static boolean waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        int waited = 0;
        while (!condition.getAsBoolean() && waited < timeout.toMillis()) {
            Thread.sleep(100L);
            waited += 100;
        }
        return condition.getAsBoolean();
    }

    /**
     * 异步确认简单测试
     */
    private static void asynchronousTest() throws Exception{
        Connection connection = null;
        Channel channel = null;
        try {
            connection = RabbitMQFactory.getConnection();
            channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, true, null);
            //开启消息确认，默认时关闭的
            channel.confirmSelect();

            ConfirmCallback ackCallback = (sequenceNumber, multiple) -> {
                System.out.println("ackCallback,sequenceNumber=" + sequenceNumber + ",multiple=" + multiple);
            };
            ConfirmCallback nackCallback = (sequenceNumber, multiple) -> {
                System.out.println("nackCallback,sequenceNumber=" + sequenceNumber + ",multiple=" + multiple);
            };
            channel.addConfirmListener(ackCallback, nackCallback);

            for (int i = 0; i < 100; i++) {
                String body = String.valueOf(i);
                channel.basicPublish("", QUEUE_NAME, null, body.getBytes());
            }

            Thread.sleep(1000 * 30);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            channel.close();
            connection.close();
        }
    }


}
