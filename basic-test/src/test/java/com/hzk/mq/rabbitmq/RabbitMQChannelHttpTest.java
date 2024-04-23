package com.hzk.mq.rabbitmq;

import com.hzk.mq.rabbitmq.factory.RabbitMQFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class RabbitMQChannelHttpTest {

    private final static String QUEUE_NAME = "work_queues_test_0";
    static Channel channel;

    public static void main(String[] args) throws Exception{
        Connection connection = RabbitMQFactory.getConnection();
        channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);



        HttpServer httpServer = HttpServer.create(new InetSocketAddress(8000), 0);
        httpServer.createContext("/test", new TestHandler());
        httpServer.start();

    }

    static class TestHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String message = "消息-1";
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");

            exchange.sendResponseHeaders(200, 0);
            OutputStream os = exchange.getResponseBody();
            os.write("success".getBytes());
            os.close();
        }

    }



}
