package com.hzk.mq.support;

import java.io.Serializable;

public class Message implements Serializable {

    private Object body;
    private long startDeliverTime;
    private int retryTimes;


    public Object getBody() {
        return body;
    }

    public void setBody(Object body) {
        this.body = body;
    }

    public long getStartDeliverTime() {
        return startDeliverTime;
    }

    public void setStartDeliverTime(long startDeliverTime) {
        this.startDeliverTime = startDeliverTime;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }
}
