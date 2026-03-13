package com.hzk.springboot.consumer;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class BatchReceiver {

    @RabbitListener(queues= "batchQueue", containerFactory = "batch")
    public void batchReceive(List<Message> messageList, Channel channel) {
        try {
            System.out.println("Received size:" + messageList.size());
            Map<Long, List<Message>> deliveryTag2messageListMap = new HashMap<>();
            for (Message tempMessage : messageList) {
                List<Message> tempMessageList = deliveryTag2messageListMap.computeIfAbsent(tempMessage.getMessageProperties().getDeliveryTag(), key -> new ArrayList<>());
                tempMessageList.add(tempMessage);
            }
            for(Map.Entry<Long, List<Message>> entry : deliveryTag2messageListMap.entrySet()) {
                long deliveryTag = entry.getKey();
                System.out.println("deliveryTag:" + deliveryTag);
                List<Message> tempMessageList = entry.getValue();
                for(Message tempMessage : tempMessageList) {
                    System.out.println("Received message:" + new String(tempMessage.getBody()));
                }
                channel.basicAck(deliveryTag, true);
                System.out.println("----------------------");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
