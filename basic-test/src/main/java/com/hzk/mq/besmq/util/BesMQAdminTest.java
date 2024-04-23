package com.hzk.mq.besmq.util;

import com.bes.mq.admin.facade.api.destinations.DestinationsFacade;
import com.bes.mq.admin.facade.api.destinations.pojo.QueuePojo;
import com.bes.mq.admin.facade.api.monitor.MonitorFacade;
import com.bes.mq.admin.facade.impl.jeemx.JMXAdminFacadeFactory;
import com.bes.mq.broker.jmx.QueueViewMBean;
import com.bes.mq.broker.jmx.SubscriptionViewMBean;
import com.bes.mq.command.BESMQTextMessage;

import java.util.Collection;
import java.util.List;

public class BesMQAdminTest {

    public static void main(String[] args) throws Exception{

        JMXAdminFacadeFactory besMQAdmin = BesMQAdminUtil.getBesMQAdmin();
        // Destinations
        String queueName = "dynamicQueueTest1";
        DestinationsFacade destinationsFacade = besMQAdmin.getDestinationsFacade();
        // 创建队列
        QueuePojo queuePojo = new QueuePojo();
        queuePojo.setName(queueName);
        destinationsFacade.createQueue(queuePojo);
        // 删除队列
        destinationsFacade.deleteQueue(queueName);

        // monitor功能
        MonitorFacade monitorFacade = besMQAdmin.getMonitorFacade();

        System.out.println("---------------------------");
        System.out.println(monitorFacade.getQueue("queueTest").getEnqueueCount());
        System.out.println(monitorFacade.getQueue("queueTest").getDequeueCount());
        System.out.println(monitorFacade.getQueue("queueTest").getQueueSize());
        System.out.println(monitorFacade.getQueue("queueTest").getConsumerCount());
        System.out.println("---------------------------");

        System.out.println(monitorFacade.getQueue("queueTest1").getEnqueueCount());
        System.out.println(monitorFacade.getQueue("queueTest1").getDequeueCount());
        System.out.println(monitorFacade.getQueue("queueTest1").getQueueSize());
        System.out.println(monitorFacade.getQueue("queueTest1").getConsumerCount());
        System.out.println("---------------------------");

        // page1：列表页
        Collection<QueueViewMBean> queueViewMBeanCollection = monitorFacade.getQueues();
        for(QueueViewMBean queueViewMBean:queueViewMBeanCollection) {
            String tempQueueName = queueViewMBean.getName();
            System.out.println(tempQueueName);
            System.out.println(queueViewMBean.getEnqueueCount());
            System.out.println(queueViewMBean.getDequeueCount());
            System.out.println(queueViewMBean.getQueueSize());
            System.out.println(queueViewMBean.getConsumerCount());

            // page2：消费者集合
            Collection<SubscriptionViewMBean> subscriptionViewMBeanCollection = monitorFacade.getQueueConsumers(tempQueueName);
            if (subscriptionViewMBeanCollection != null && subscriptionViewMBeanCollection.size() > 0) {
                for(SubscriptionViewMBean tempSubscriptionMBean : subscriptionViewMBeanCollection) {
//                    System.out.println(tempSubscriptionMBean.getConnectionId());
                    System.out.println(tempSubscriptionMBean.getSessionId());
//                    System.out.println(tempSubscriptionMBean.getClientId());
                    System.out.println(tempSubscriptionMBean.getSubcriptionId());
//                    System.out.println(tempSubscriptionMBean.getSelector());
                    System.out.println(tempSubscriptionMBean.getEnqueueCounter());
                    System.out.println(tempSubscriptionMBean.getDequeueCounter());
                    System.out.println(tempSubscriptionMBean.getDispatchedCounter());
                    System.out.println(tempSubscriptionMBean.getDispatchedQueueSize());
                    System.out.println(tempSubscriptionMBean.getMessageCountAwaitingAcknowledge());
//                    System.out.println(tempSubscriptionMBean.getPrefetchSize());
//                    System.out.println(tempSubscriptionMBean.isExclusive());
                    System.out.println(tempSubscriptionMBean.isActive());
                    System.out.println(tempSubscriptionMBean);
                }
            }
            System.out.println("---------------------------");
        }

        besMQAdmin.clear();
    }

}
