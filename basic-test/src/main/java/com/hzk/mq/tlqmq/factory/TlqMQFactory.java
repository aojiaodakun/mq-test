package com.hzk.mq.tlqmq.factory;

import javax.jms.Connection;

import com.tongtech.tmqi.ConnectionFactory;

public class TlqMQFactory {

    private static final int TLQ_ADMIN_PORT = 10252;
    private static final int JMS_DEFAULT_PORT = 3100;
    private static final String TLQ_DEFAULT_QCU_NAME = "qcu1";
    private static final String MQ_TLQ_QUEUE_PREFETCH = "mq.tlq.queue.prefetch";
    private static final String MQ_TLQ_QUEUE_PREFETCH_VAL = "100";
    private static final String TMQI_ADDRESS_LIST = "tmqiAddressList";
    private static final String TMQI_DEFAULT_USERNAME = "tmqiDefaultUsername";
    private static final String TMQI_DEFAULT_PASSWORD_KEY = "tmqiDefaultPassword";
    private static final String TMQI_RECONNECT_ENABLED = "tmqiReconnectEnabled";
    private static final String TMQI_CONFIGURED_CLIENT_ID = "tmqiConfiguredClientID";
    private static final String TMQI_CONSUMER_FLOW_LIMIT = "tmqiConsumerFlowLimit";
    private static final String TMQI_ACK_TIMEOUT = "tmqiAckTimeout";
    private static final String MQ_TLQ_ACK_TIMEOUT = "mq.tlq.ack.timeout";
    private static final String MQ_TLQ_ACK_TIMEOUT_VAL = "1800000";
    private static final String TMQI_CONSUMER_FLOW_THRESHOLD = "tmqiConsumerFlowThreshold";
    private static final String MQ_TLQ_TMQI_CONSUMER_FLOW_THRESHOLD = "mq.tlq.tmqiConsumerFlowThreshold";
    private static final String TMQI_PRODUCER_FLOW_BYTES_LIMIT = "tmqiProducerFlowBytesLimit";
    private static final String MQ_TLQ_TMQI_PRODUCER_FLOW_BYTES_LIMIT = "mq.tlq.tmqiProducerFlowBytesLimit";
    private static Connection connection;
    private static final Object LOCKER = new Object();


    public static Connection getConnection() throws Exception {
        if (connection == null) {
            synchronized (LOCKER) {
                if (connection == null) {
                    ConnectionFactory connectionFactory = new com.tongtech.tmqi.ConnectionFactory();
//                    connectionFactory.setProperty(TMQI_ADDRESS_LIST, "tlq://127.0.0.1:10024");
                    connectionFactory.setProperty(TMQI_ADDRESS_LIST, "tlq://172.20.158.201:10024");
                    connectionFactory.setProperty(TMQI_DEFAULT_USERNAME, "guest");
                    connectionFactory.setProperty(TMQI_DEFAULT_PASSWORD_KEY, "guest");
                    connectionFactory.setProperty(TMQI_RECONNECT_ENABLED, "true");
                    connectionFactory.setProperty(TMQI_CONFIGURED_CLIENT_ID, "hzk-1");
                    connectionFactory.setProperty(TMQI_CONSUMER_FLOW_LIMIT, System.getProperty(MQ_TLQ_QUEUE_PREFETCH, MQ_TLQ_QUEUE_PREFETCH_VAL));//prefect  ，消费者一次性发送到客户端的数量
                    connectionFactory.setProperty(TMQI_CONSUMER_FLOW_THRESHOLD, System.getProperty(MQ_TLQ_TMQI_CONSUMER_FLOW_THRESHOLD, "50"));//消费者流控小于50%时恢复，范围[1,99]
                    connectionFactory.setProperty(TMQI_ACK_TIMEOUT, System.getProperty(MQ_TLQ_ACK_TIMEOUT, MQ_TLQ_ACK_TIMEOUT_VAL));//ack timeout
                    connectionFactory.setProperty(TMQI_PRODUCER_FLOW_BYTES_LIMIT, System.getProperty(MQ_TLQ_TMQI_PRODUCER_FLOW_BYTES_LIMIT, "10000000"));//producer流控单位byte
                    connection = connectionFactory.createConnection();
                    connection.start();
                }
            }
        }
        return connection;
    }

}
