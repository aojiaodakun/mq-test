//package com.hzk.mq.rocketmq.log;
//
//import org.apache.commons.io.Charsets;
//import org.apache.rocketmq.client.producer.DefaultMQProducer;
//import org.apache.rocketmq.common.logging.JoranConfiguratorExt;
//import org.apache.rocketmq.logging.ch.qos.logback.classic.LoggerContext;
//import org.apache.rocketmq.logging.ch.qos.logback.classic.joran.JoranConfigurator;
//import org.apache.rocketmq.logging.org.slf4j.ILoggerFactory;
//import org.apache.rocketmq.logging.org.slf4j.Logger;
//import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
//
//import java.io.ByteArrayInputStream;
//import java.io.InputStream;
//import java.net.URL;
//
//public class RocketMQLogTest {
//
//    static {
//        // 关键：设置 RocketMQ 使用 SLF4J
//        System.setProperty("rocketmq.client.logUseSlf4j", "true");
//        System.setProperty("rocketmq.client.logger", "slf4j");
//        System.setProperty("rocketmq.client.log.adapter", "slf4j");
//        System.setProperty("rocketmq.log.console.appender.enabled", "false");
//    }
//
//    public static void main(String[] args) throws Exception {
//        Logger logger = LoggerFactory.getLogger(RocketMQLogTest.class);
//        logger.info("aaa");
//
//        /**
//         * 用苍穹的log.config覆盖
//         * rocketmq源码：org.apache.rocketmq.common.logging.DefaultJoranConfiguratorExt#configureByResource(java.net.URL)
//         * 苍穹源码：kd.bos.logging.logback.LogbackFactory#initConfig()
//         */
//        // 更改logback日志配置
//        ILoggerFactory iLoggerFactory = LoggerFactory.getILoggerFactory();
//        if (iLoggerFactory instanceof LoggerContext) {
//            LoggerContext loggerContext = (LoggerContext)iLoggerFactory;
//
//            JoranConfiguratorExt configurator = new JoranConfiguratorExt();
//            configurator.setContext(loggerContext);
//            loggerContext.reset();
//            try {
//                URL logbackURL = RocketMQLogTest.class.getResource("/logback.xml");
//                configurator.doConfigure0(logbackURL);
//            }  catch (org.apache.rocketmq.logging.ch.qos.logback.core.joran.spi.JoranException ex) {
//                System.out.println("-----rocketmq log.config doConfigure error-----");//NOSONAR
//                ex.printStackTrace();//NOSONAR
//            }
//        }
//
//        logger.info("bbb");
//    }
//
//}
