package com.hzk.mq.support.delay;

import com.hzk.mq.support.Message;

import java.util.Calendar;

public class DelayControlManager {

    public static final int[] supportMetaTime = {1, 5, 10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200};
    private static final int SUPPORT_META_TIME_MIN_LEVEL = 1;
    private static final String SUPPORT_MIN_SECOND="mq.delay.min.second";
    private static final String SUPPORT_MAX_SECOND ="mq.delay.max.second";

    public static void installDelayInfo(Message message, int seconds) {
        int minDelayTime = Integer.getInteger(SUPPORT_MIN_SECOND, 5);

        if (seconds >= minDelayTime) {
            validateTime(seconds);
            message.setStartDeliverTime(convertTime(seconds));
        }
    }

    /**
     * 获取真实topic投递时间点
     * @param seconds 延迟秒数
     * @return 真实topic投递时间点
     */
    public static long getStartDeliverTime(int seconds) {
        int minDelayTime = Integer.getInteger(SUPPORT_MIN_SECOND, 5);
        if (seconds >= minDelayTime) {
            validateTime(seconds);
            return convertTime(seconds);
        }
        return 0L;
    }

    private static void validateTime(int seconds) {
        int maxDelayTime = Integer.getInteger(SUPPORT_MAX_SECOND, 7200);

        if (seconds > maxDelayTime) {
            throw new IllegalArgumentException("max delay time is "+maxDelayTime+" seconds");
        }
    }

    /**
     * 剩余时间的最大元时间计算；
     *
     * @param remainSeconds
     * @return
     */
    public static MetaTime selectMaxMetaTime(int remainSeconds) {
        int level = -1;
        for (int i = supportMetaTime.length - 1; i >= 0; i--) {
            if (supportMetaTime[i] <= remainSeconds) {
                level = i + SUPPORT_META_TIME_MIN_LEVEL;
                break;
            }
        }
        MetaTime metaTime = MetaTime.genInstanceByLevel(level);
        return metaTime == null ? MetaTime.delay_1s : metaTime;
    }


    public static long convertTime(int seconds) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.SECOND, seconds);
        return calendar.getTime().getTime();
    }


    public static void main(String[] args) {
        //     private static final int[] supportMetaTime = {1, 5, 10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200};
        MetaTime metaTime = selectMaxMetaTime(7200);
        metaTime = selectMaxMetaTime(3600);
        metaTime = selectMaxMetaTime(1800);
        metaTime = selectMaxMetaTime(1200);
        metaTime = selectMaxMetaTime(600);
        metaTime = selectMaxMetaTime(540);


        //
        metaTime = selectMaxMetaTime(1500);
        metaTime = selectMaxMetaTime(200);
        metaTime = selectMaxMetaTime(90);

        System.out.println(metaTime);

    }

}
