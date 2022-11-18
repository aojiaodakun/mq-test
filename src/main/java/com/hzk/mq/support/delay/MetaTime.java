package com.hzk.mq.support.delay;

public enum MetaTime {

    //rocket messageDelayLevel
    //1s 5s 10s 30s 1m 2m 3m 4m 5m 6m  7m  8m 9m 10m 20m  30m 1h 2h
    //1  2   3   4  5  6  7  8  9  10  11  12 13  14  15  16  17 18

    delay_1s(1000, 1, "delay_1s"),

    delay_5s(5000, 2, "delay_5s"),

    delay_10s(10000, 3, "delay_10s"),

    delay_30s(30000, 4, "delay_30s"),

    delay_1m(60000, 5, "delay_1m"),

    delay_2m(120000, 6, "delay_2m"),

    delay_3m(180000, 7, "delay_3m"),

    delay_4m(240000, 8, "delay_4m"),

    delay_5m(300000, 9, "delay_5m"),

    delay_6m(360000, 10, "delay_6m"),

    delay_7m(420000, 11, "delay_7m"),

    delay_8m(480000, 12, "delay_8m"),

    delay_9m(540000, 13, "delay_9m"),

    delay_10m(600000, 14, "delay_10m"),

    delay_20m(1200000, 15, "delay_20m"),

    delay_30m(1800000, 16, "delay_30m"),

    delay_1h(3600000, 17, "delay_1h"),

    delay_2h(7200000, 18, "delay_2h");

    private int millis;
    private int level;
    private String name;

    MetaTime(int millis, int level, String name) {
        this.millis = millis;
        this.level = level;
        this.name = name;
    }

    public int getMillis() {
        return millis;
    }

    public int getLevel() {
        return level;
    }

    public String getName() {
        return name;
    }

    public static MetaTime genInstanceByLevel(int level) {
        switch (level) {
            case 1:
                return delay_1s;
            case 2:
                return delay_5s;
            case 3:
                return delay_10s;
            case 4:
                return delay_30s;
            case 5:
                return delay_1m;
            case 6:
                return delay_2m;
            case 7:
                return delay_3m;
            case 8:
                return delay_4m;
            case 9:
                return delay_5m;
            case 10:
                return delay_6m;
            case 11:
                return delay_7m;
            case 12:
                return delay_8m;
            case 13:
                return delay_9m;
            case 14:
                return delay_10m;
            case 15:
                return delay_20m;
            case 16:
                return delay_30m;
            case 17:
                return delay_1h;
            case 18:
                return delay_2h;
        }
        return null;
    }

}
