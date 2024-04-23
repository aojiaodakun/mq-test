### mq测试工程
| mq | 基础功能 | 新增特性 |
| :------ | :------ | :------ |
| activemq | 发布订阅 | - |
| kafka | 发布订阅 | 延迟消息，消费重试 |
| rabbitmq | 发布订阅 | - |
| rocketmq | 发布订阅 | - |

### 一、activemq
#### 1、windows单机部署
1.1、官网下载，版本5.16.5，apache-activemq-5.16.5-bin.zip

https://activemq.apache.org/components/classic/download/

1.2、配置修改

> /conf/activemq.xml

transportConnectors标签，此处可修改端口

1.3、启动服务

/bin/win64/activemq.bat

1.4、控制台地址

http://localhost:8161/

1.5、快速启动

根路径下新建start.bat

.\bin\win64\activemq.bat

---

### 二、kafka
#### 1、kafka在windows单机部署

参考：https://baijiahao.baidu.com/s?id=1730410485323023789&wfr=spider&for=pc

1.1、官网下载，版本kafka_2.12-3.1.0。kafka_scala_java
https://kafka.apache.org/downloads

1.2、启动内置zookeeper，端口2181
> 改配置

根路径创建zkData文件夹；
/config/zookeeper.properties
dataDir=D:\\tool\\kafka_2.12-3.1.0\\zkData

> 启动单机zookeeper

根路径下执行
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

1.3、启动单机kafka，端口9092

> 改配置

根路径创建logs文件夹；
/config/server.properties
host.name=localhost
listeners=PLAINTEXT://:9092
log.dirs=D:\\tool\\kafka_2.12-3.1.0\\logs
auto.create.topics.enable=false

> 启动单机kafka

.\bin\windows\kafka-server-start.bat .\config\server.properties

1.4、创建topic=test

根路径下执行
.\bin\windows\kafka-topics.bat --create --bootstrap-server  localhost:2181 --replication-factor 1 --partitions 1 --topic test

1.5、五、启动消费者

根路径下执行
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test

1.6、启动生产者

.\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic test

1.7、快速启动

>zk

根路径新建startZkServer.bat

.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

>kafka

根路径新建startKafkaServer.bat

.\bin\windows\kafka-server-start.bat .\config\server.properties

---

### 三、rabbitmq
#### 1、rabbitmq在windows单机部署
1.1、官网下载，版本3.8.18，可设置系统变量RABBITMQ_SERVER

https://www.rabbitmq.com/news.html

下载依赖erlang-23.2，需设置系统变量ERLANG_HOME

https://www.erlang.org/patches/otp-23.2

1.2、安装插件

/sbin目录执行cmd

rabbitmq-plugins enable rabbitmq_management

1.3、启动服务

/sbin目录执行cmd

rabbitmq-server.bat start

1.4、控制台地址

http://localhost:15672/#/

1.5、快速启动

根路径新建start.bat

.\sbin\rabbitmq-server.bat start

1.6、常用命令

查询队列

rabbitmqctl list_queues

关闭应用

rabbitmqctl stop_app

执行清除命令

rabbitmqctl reset

启动应用

rabbitmqctl start_app

---

### 四、rocketmq
#### 1、rocketmq在windows单机部署
#### 1.1、官网下载，版本4.9.3
https://rocketmq.apache.org/download/

#### 1.2、配置修改

> 0、系统变量

ROCKETMQ_HOME=D:\tool\rocketmq-4.9.3

> 1、namesrv

根路径新增namesrvhome文件夹
/bin/runserver.cmd
set "JAVA_OPT=%JAVA_OPT% -Duser.home=D:\tool\rocketmq-4.9.3\namesrvhome"

> 2、broker

根路径新增brokerhome文件夹
/bin/runbroker.cmd
set "JAVA_OPT=%JAVA_OPT% -Duser.home=D:\tool\rocketmq-4.9.3\brokerhome"

#### 1.3、启动namesrv，端口9876
执行bin/mqnamesrv.cmd

#### 1.4、启动broker，端口10911
执行bin/mqbroker.cmd -n localhost:9876

#### 1.5、tools.cmd启动消费者
set NAMESRV_ADDR=localhost:9876

tools.cmd org.apache.rocketmq.example.quickstart.Consumer

#### 1.6、tools.cmd启动生产者
set NAMESRV_ADDR=localhost:9876

tools.cmd org.apache.rocketmq.example.quickstart.Producer

#### 2、rocketmq-dashboard在windows单机部署，端口8999
https://github.com/apache/rocketmq-dashboard/releases/tag/rocketmq-dashboard-1.0.0
下载Source code(zip)

2.1、maven打包

根目录，cmd，执行mvn clean install -Dmaven.test.skip=true

2.2、启动控制台

根目录，cmd，java -Drocketmq.namesrv.addr=127.0.0.1:9876 -Dserver.port=8999 -jar .\target\rocketmq-dashboard-1.0.0.jar

2.3、访问控制台

localhost:8999