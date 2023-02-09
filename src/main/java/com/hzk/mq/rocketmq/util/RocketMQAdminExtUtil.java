package com.hzk.mq.rocketmq.util;

import com.hzk.mq.rocketmq.constants.RocketMQConstants;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

public class RocketMQAdminExtUtil {

    private static DefaultMQAdminExt mqAdminExt;

    private static final Object LOCKER = new Object();

    private static final String NAMESRV_ADDR = System.getProperty(RocketMQConstants.MQ_ROCKETMQ_NAMESRVADDR_DEFAULT, "localhost:9876");

    public static DefaultMQAdminExt getMQAdminExt(){
        initMQAdminExt();
        return mqAdminExt;
    }

    private static void initMQAdminExt(){
        if (mqAdminExt != null) {
            return;
        }
        synchronized (LOCKER) {
            if (mqAdminExt != null) {
                return;
            }
            mqAdminExt = new DefaultMQAdminExt(5000L);
            mqAdminExt.setNamesrvAddr(NAMESRV_ADDR);
            mqAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
            try {
                mqAdminExt.start();
            } catch (MQClientException e) {
                e.printStackTrace();
            }
        }
    }



}
