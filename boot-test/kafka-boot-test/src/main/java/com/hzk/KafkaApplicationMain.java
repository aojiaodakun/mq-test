package com.hzk;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaApplicationMain {

    /**
     * bootstrap.servers=localhost:9092 topic=testConsole group=testConsole
     * bootstrap.servers=172.17.7.78:9092 topic=testConsole group=testConsole
     * @param args
     */
    public static void main(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String[] tempArr = args[i].split("=");
            System.setProperty(tempArr[0], tempArr[1]);
        }
        SpringApplication.run(KafkaApplicationMain.class, args);
    }

}
