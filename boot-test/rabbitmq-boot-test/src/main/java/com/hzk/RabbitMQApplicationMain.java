package com.hzk;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultSaslConfig;
import com.rabbitmq.client.LongString;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.impl.LongStringHelper;
import org.apache.commons.io.FilenameUtils;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Objects;

/**
 * type=rabbitmq
 * host=172.17.8.198
 * port=5671
 * user=rabbitmq-client
 * vhost=perf-reliability
 * ssl.enable=true
 * certificate.enable=true
 * ssl.certificate.clientPath=/tmp/nfssharedata/rabbitmq/ssl/rabbitmq-client.keycert.p12
 * ssl.certificate.trustPath=/tmp/nfssharedata/rabbitmq/ssl/trustStore
 * ssl.certificate.keyPassword=Kingdee#123
 * ssl.certificate.trustPassword=Kingdee#123
 * auth.mechanism=EXTERNAL
 * queue=demo.demo_queue
 */
public class RabbitMQApplicationMain {


    public static void main(String[] args) throws Exception{
        byte[] bytes = new byte[]{101,110,95,85,83};
        String s = new String(bytes);
        LongString longString = LongStringHelper.asLongString(bytes);

        for (int i = 0; i < args.length; i++) {
            String[] tempArr = args[i].split("=");
            System.setProperty(tempArr[0], tempArr[1]);
        }
        producerTest();
    }

    private static void producerTest() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(System.getProperty("host"));
        factory.setPort(Integer.getInteger("port"));
        String user = System.getProperty("user");
        if (user != null) {
            factory.setUsername(user);
        }
        String password = System.getProperty("password");
        if (password != null) {
            factory.setPassword(password);
        }
        factory.setVirtualHost(System.getProperty("vhost"));
        factory.setChannelRpcTimeout(60000);
        factory.setConnectionTimeout(3000);
        /**
         * SSL
         */
        String sslEnable = System.getProperty("ssl.enable", "false");
        if (Boolean.parseBoolean(sslEnable)) {
            String certificate = System.getProperty("certificate.enable", "false");
            if (Boolean.parseBoolean(certificate)) {
                String clientPath = checkFileUrl(Objects.requireNonNull(System.getProperty("ssl.certificate.clientPath"), "ssl.certificate.clientPath can't be empty."));
                String trustPath = checkFileUrl(Objects.requireNonNull(System.getProperty("ssl.certificate.trustPath"), "ssl.certificate.trustPath can't be empty."));
                String sslClientPassword = Objects.requireNonNull(System.getProperty("ssl.certificate.keyPassword"), "ssl.certificate.keyPassword can't be empty.");
                String sslTruststorePassword = Objects.requireNonNull(System.getProperty("ssl.certificate.trustPassword"), "ssl.certificate.trustPassword can't be empty.");
                SSLContext c = initSslSupport(clientPath, trustPath, sslClientPassword, sslTruststorePassword);
                factory.useSslProtocol(c);
            } else {
                factory.useSslProtocol();
            }
        }
        if("EXTERNAL".equalsIgnoreCase(System.getProperty("auth.mechanism"))){
            factory.setSaslConfig(DefaultSaslConfig.EXTERNAL);
        }
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        String queue = System.getProperty("queue");
        channel.queueDeclare(queue, true, false, false, null);
        for (int i = 0; i < 10; i++) {
            String message = "消息-" + i;
            channel.basicPublish("", queue, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
        }
        Thread.currentThread().sleep(1000 * 60);
        // close
        channel.close();
        connection.close();
    }

    public static String checkFileUrl(String fileUrl) {
        return fileUrl==null||fileUrl.equals("") ? fileUrl : FilenameUtils.normalize(fileUrl);
    }

    private static SSLContext initSslSupport(String clientPath, String trustPath, String sslClientPassword, String sslTruststorePassword) {
        SSLContext sslContext = null;
        //FileInputStream clientPathStream = null;
        //FileInputStream trustPathStream = null;
        try(FileInputStream clientPathStream = new FileInputStream(clientPath);//NOSONAR
            FileInputStream trustPathStream = new FileInputStream(trustPath)) {//NOSONAR
            char[] clientPassphrase = sslClientPassword.trim().toCharArray(); //证书客户端密码
            KeyStore ks = KeyStore.getInstance("PKCS12");
            //ks.load(RabbitmqFactory.class.getClassLoader().getResourceAsStream("kd_rabbit-client.keycert.p12"), keyPassphrase);
            //clientPathStream = new FileInputStream(clientPath);//NOSONAR
            ks.load(clientPathStream, clientPassphrase);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(ks, clientPassphrase);
            char[] trustPassphrase = sslTruststorePassword.trim().toCharArray();//证书服务端密码
            KeyStore tks = KeyStore.getInstance("JKS");
//            tks.load(RabbitmqFactory.class.getClassLoader().getResourceAsStream("trustStore"), trustPassphrase);
            //trustPathStream = new FileInputStream(trustPath);//NOSONAR
            tks.load(trustPathStream, trustPassphrase);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            tmf.init(tks);
            sslContext = SSLContext.getInstance("TLSv1.2");
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        } catch (IOException | NoSuchAlgorithmException | CertificateException | UnrecoverableKeyException
                | KeyStoreException | KeyManagementException e) {
            e.printStackTrace();
        }
        return sslContext;
    }

}
