package com.hzk.mq.rocketmq.log;

//import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;

import org.apache.rocketmq.logging.ch.qos.logback.classic.spi.ILoggingEvent;
//import org.apache.rocketmq.logging.ch.qos.logback.core.UnsynchronizedAppenderBase;

public class KafkaAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {
    @Override
    protected void append(ILoggingEvent eventObject) {
        System.err.println("kafka:" + eventObject.getMessage());
    }
}
