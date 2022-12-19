package com.hzk.mq.kafka;

public class KafkaAcker {

    private int visitStatus = 0;

    private int ackStatus = 0;// ack 1, deny 2, discard 3

    public int getAckStatus() {
        return ackStatus;
    }

    public void setAckStatus(int ackStatus) {
        this.ackStatus = ackStatus;
    }

    public boolean hasDone() {
        return visitStatus > 0;
    }

    public void ack() {
        if (visitStatus++ > 0) {
            return;
        }
        setAckStatus(1);
    }

    public void deny() {
        if (visitStatus++ > 0) {
            return;
        }
        setAckStatus(2);
    }

    public void discard() {
        if (visitStatus++ > 0) {
            return;
        }
        setAckStatus(3);
    }

    public boolean isDenied()
    {
        return ackStatus == 2;
    }


    public boolean isAcked() {
        return ackStatus == 1;
    }

    public boolean isDiscarded() {
        return ackStatus == 3;
    }

}
