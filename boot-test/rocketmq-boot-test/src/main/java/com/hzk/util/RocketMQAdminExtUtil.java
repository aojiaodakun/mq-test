package com.hzk.util;

import com.hzk.constants.RocketMQConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.RPCHook;
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
            mqAdminExt = new DefaultMQAdminExt(getAclRPCHook(), 1000 * 10);
            mqAdminExt.setNamesrvAddr(NAMESRV_ADDR);
            mqAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
            try {
                mqAdminExt.start();
            } catch (MQClientException e) {
                e.printStackTrace();
            }
        }
    }


    public static RPCHook getAclRPCHook() {
        String accessKey = System.getProperty("user");
        String secretKey = System.getProperty("password");
        return !StringUtils.isEmpty(accessKey) && !StringUtils.isEmpty(secretKey) ?
                new AclClientRPCHook(new SessionCredentials(accessKey, secretKey)) : null;
    }



}
