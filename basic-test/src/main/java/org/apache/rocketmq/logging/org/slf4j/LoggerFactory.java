package org.apache.rocketmq.logging.org.slf4j;

public class LoggerFactory {

    private static final ILoggerFactory FACTORY = new Slf4jDelegateLoggerFactory();

    public static Logger getLogger(String name) {
        return new Slf4jDelegateLogger(org.slf4j.LoggerFactory.getLogger(name));
    }

    public static Logger getLogger(Class<?> clazz) {
        return new Slf4jDelegateLogger(org.slf4j.LoggerFactory.getLogger(clazz));
    }

    public static ILoggerFactory getILoggerFactory() {
        return FACTORY;
    }


}
