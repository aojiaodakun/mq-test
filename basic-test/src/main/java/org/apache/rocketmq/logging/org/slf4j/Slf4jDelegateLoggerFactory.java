package org.apache.rocketmq.logging.org.slf4j;

public class Slf4jDelegateLoggerFactory implements ILoggerFactory {

    @Override
    public Logger getLogger(String name) {
        org.slf4j.Logger real = org.slf4j.LoggerFactory.getLogger(name);
        return new Slf4jDelegateLogger(real);
    }
}