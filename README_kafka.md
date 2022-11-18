### kafka在windows单机部署

参考：https://baijiahao.baidu.com/s?id=1730410485323023789&wfr=spider&for=pc

#### 一、官网下载，版本kafka_2.12-3.1.0。kafka_scala_java
https://kafka.apache.org/downloads

#### 二、启动内置zookeeper，端口2181
> 1、改配置

根路径创建zkData文件夹；
/config/zookeeper.properties
dataDir=D:\\tool\\kafka_2.12-3.1.0\\zkData

> 2、启动单机zookeeper

根路径下执行
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

#### 三、启动单机kafka，端口9092

> 1、改配置

根路径创建logs文件夹；
/config/server.properties
listeners=PLAINTEXT://:9092
log.dirs=D:\\tool\\kafka_2.12-3.1.0\\logs
auto.create.topics.enable=true

> 2、启动单机kafka

.\bin\windows\kafka-server-start.bat .\config\server.properties

#### 四、创建topic=test

根路径下执行
.\bin\windows\kafka-topics.bat --create --bootstrap-server  localhost:2181 --replication-factor 1 --partitions 1 --topic test

#### 五、启动消费者

根路径下执行
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test

#### 六、启动生产者

.\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic test