package com.hzk.mq.rabbitmq.factory;

import com.rabbitmq.client.Channel;

public class ChannelFactory {

    public static boolean isChannelNeedReBuild(Channel channel) {
        boolean needRebuild = channel == null || !channel.isOpen() || !channel.getConnection().isOpen();
        if (needRebuild && channel != null) {
            try {
                channel.close();
            } catch (Exception var3) {
            }
        }
        return needRebuild;
    }

}
