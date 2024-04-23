package com.hzk.mq.besmq.util;

import com.bes.mq.admin.facade.api.BrokerInfo;
import com.bes.mq.admin.facade.impl.jeemx.JEEMXBasedAdminFacadeFactory;
import com.bes.mq.admin.facade.impl.jeemx.JMXAdminFacadeFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class BesMQAdminUtil {

    private static JMXAdminFacadeFactory adminFacadeFactory;

    private static AtomicBoolean isStart = new AtomicBoolean(false);

    private static void init(){
        BrokerInfo brokerInfo = new BrokerInfo();
        brokerInfo.setHost("127.0.0.1");
        brokerInfo.setPort(3100);
        brokerInfo.setUsername("admin");
        brokerInfo.setPassword("admin");
        adminFacadeFactory = JEEMXBasedAdminFacadeFactory.newFactory(brokerInfo);
        isStart.compareAndSet(false, true);
    }

    public static JMXAdminFacadeFactory getBesMQAdmin() {
        if (!isStart.get()) {
            init();
        }
        return adminFacadeFactory;
    }

}
