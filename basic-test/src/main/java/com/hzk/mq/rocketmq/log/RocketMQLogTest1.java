package com.hzk.mq.rocketmq.log;

import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class RocketMQLogTest1 {

    public static void main(String[] args) throws Exception {

        Logger logger = LoggerFactory.getLogger(RocketMQLogTest1.class);
        logger.info("aaa");

    }

}
