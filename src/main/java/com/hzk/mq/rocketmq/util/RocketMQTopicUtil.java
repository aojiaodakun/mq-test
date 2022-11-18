package com.hzk.mq.rocketmq.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.topic.UpdateTopicSubCommand;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * 若开启了自动创建TOPIC(autoCreateTopicEnable=true),默认创建一个TOPIC,TOPIC绑定读写各四条队列(可在客户端手动更改).
 * 注意：只有生产者往对应的TOPIC发消息时才会自动创建。官网建议线下开启，线上关闭自动创建，
 * 因为可能第一次发的消息数量不够队列数量，只会在一个节点上创建TOPIC达不到负载均衡。
 * 所以在以上背景下，通过此API在初始化Consumer的时候就在各个Broker上把Topic创建好
 */
public class RocketMQTopicUtil {

    private static Map<String, Set<String>> clusterNameMap = new ConcurrentHashMap<>(2);

    /**
     * 在指定name server下使用默认参数创建topic
     *
     * @param namesrvAddr
     * @param topic
     * @return
     */
    public static boolean createTopic(String namesrvAddr, String topic, int queueNums, RPCHook rpcHook) {
        try {
            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, namesrvAddr);
            Set<String> clusterNames = getClusterNames(namesrvAddr);
            for (String clusterName : clusterNames) {
                createTopic(null, clusterName, topic, queueNums, rpcHook);
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("error when RocketMQTopicUtil createTopic," +
                    "namesrvAddr:"+namesrvAddr+",topic:"+topic, e);
        }
    }

    /**
     * 创建topic 可以自定义所有topic支持的参数
     *
     * @param subargs updateTopic命名支持的所有参数选项
     * @return topic创建成功，返回 true
     * @throws SubCommandException
     */
    private static boolean createTopic(String[] subargs, RPCHook rpcHook) {
        /*String[] subargs = new String[] {
                "-b 10.1.4.231:10911",
                "-t unit-test-from-java-1",
                "-r 8",
                "-w 8",
                "-p 6",
                "-o false",
                "-u false",
                "-s false"};*/
        try {
            UpdateTopicSubCommand cmd = new UpdateTopicSubCommand();
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            final Options updateTopicOptions = cmd.buildCommandlineOptions(options);
            final CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(),
                    subargs, updateTopicOptions, new PosixParser());
            cmd.execute(commandLine, updateTopicOptions, rpcHook);
        } catch (SubCommandException e) {
            e.printStackTrace();
            throw new RuntimeException("error when RocketMQTopicUtil UpdateTopicSubCommand.execute", e);
        }
        return true;
    }

    /**
     * 根据 brokerAddr or clusterName 创建topic
     *
     * @param brokerAddr  在指定 broker 上创建topic时，此参数为必填，否则传null
     * @param clusterName 在指定 cluster 上创建topic时，此参数为必填，否则传null
     * @param topic       要创建的topic
     * @return 创建成功，返回true
     */
    private static boolean createTopic(String brokerAddr, String clusterName, String topic, int queueNums, RPCHook rpcHook) {
        if (StringUtils.isBlank(topic)) {
            return false;
        }
        List<String> argList = new LinkedList<>();
        argList.add("-t " + topic);
        if (StringUtils.isNotBlank(brokerAddr)) {
            argList.add("-b " + brokerAddr.trim());
        } else {
            argList.add("-c " + clusterName.trim());
        }
        if (queueNums != 0) {
            // 读队列数
            argList.add("-r " + queueNums);
            // 写队列数
            argList.add("-w " + queueNums);
        }
        return createTopic(argList.toArray(new String[0]), rpcHook);
    }

    /**
     * 获取指定 namesrv下的集群信息
     *
     * @param namesrvAddr
     * @return
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     */
    private static ClusterInfo getClusterInfo(String namesrvAddr) {
        if (StringUtils.isBlank(namesrvAddr)) {
            return new ClusterInfo();
        }
        ClusterInfo clusterInfo = null;
        try {
            DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt(5000L);
            mqAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
            mqAdminExt.setNamesrvAddr(namesrvAddr);
            mqAdminExt.start();
            clusterInfo = mqAdminExt.examineBrokerClusterInfo();
            mqAdminExt.shutdown();
        } catch (InterruptedException | RemotingConnectException | RemotingSendRequestException
                | MQBrokerException | MQClientException | RemotingTimeoutException e) {
            e.printStackTrace();
            throw new RuntimeException("error when RocketMQTopicUtil getClusterInfo,namesrvAddr:"+namesrvAddr, e);
        }
        return clusterInfo;
    }

    /**
     * 获取指定name server下的所有集群名称
     *
     * @param namesrvAddr
     * @return
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     * @throws RemotingConnectException
     */
    private static Set<String> getClusterNames(String namesrvAddr){
        Set<String> clusterNames = clusterNameMap.get(namesrvAddr);
        if (clusterNames != null) {
            return clusterNames;
        } else {
            clusterNames = getClusterInfo(namesrvAddr).getClusterAddrTable().keySet();
            clusterNameMap.put(namesrvAddr, clusterNames);
        }
        return clusterNames;
    }

    /**
     * 获取指定 namesrv 下的所有broker信息（多name server下不确定能否正常工作）
     *
     * @param namesrvAddr namesrv地址
     * @return HashMap<String, BrokerData>
     */
    private static Map<String, BrokerData> getAllBrokerInfo(String namesrvAddr){
        return getClusterInfo(namesrvAddr).getBrokerAddrTable();
    }

    /**
     * 获取连接到指定 namesrv 下的所有broker地址
     *
     * @param namesrvAddr
     * @return
     */
    private static Set<String> getBrokerAddrs(String namesrvAddr){
        Map<String, BrokerData> allBrokerInfo = getAllBrokerInfo(namesrvAddr);
        Set<String> brokerAddrs = new HashSet<>();
        for (BrokerData brokerData : allBrokerInfo.values()) {
            brokerAddrs.addAll(brokerData.getBrokerAddrs().values());
        }
        return brokerAddrs;
    }


    public static void main(String[] args) {
        boolean createTopic = RocketMQTopicUtil.createTopic("localhost:9876", "11-16-test1", 1, null);
        System.out.println(createTopic);

    }

}
