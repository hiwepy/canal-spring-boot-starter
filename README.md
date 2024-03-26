# canal-spring-boot-starter

#### 组件简介

> 基于 [Canal ](https://github.com/alibaba/canal) 整合的 Starter

> canal [kə'næl]，译意为水道/管道/沟渠，主要用途是基于 MySQL 数据库增量日志解析，提供增量数据订阅和消费

早期阿里巴巴因为杭州和美国双机房部署，存在跨机房同步的业务需求，实现方式主要是基于业务 trigger 获取增量变更。从 2010 年开始，业务逐步尝试数据库日志解析获取增量变更进行同步，由此衍生出了大量的数据库增量订阅和消费业务。

基于日志增量订阅和消费的业务包括

- 数据库镜像
- 数据库实时备份
- 索引构建和实时维护(拆分异构索引、倒排索引等)
- 业务 cache 刷新
- 带业务逻辑的增量数据处理

> 当前的 canal 支持源端 MySQL 版本包括 5.1.x , 5.5.x , 5.6.x , 5.7.x , 8.0.x

#### 使用说明

##### 1、Spring Boot 项目添加 Maven 依赖

``` xml
<dependency>
	<groupId>com.github.hiwepy</groupId>
	<artifactId>canal-spring-boot-starter</artifactId>
	<version>${project.version}</version>
</dependency>
```

##### 2、使用示例

###### 2.1、根据实际业务需求选择不同的客户端模式

在`application.yml`文件中增加如下配置

```yaml
#########################################################################################################################################################
###Canal (CanalProperties、CanalKafkaProperties、CanalRabbitMQProperties、CanalRocketMQProperties、CanalPulsarProperties、CanalTcpProperties) 基本配置：
#########################################################################################################################################################
canal:
  # Canal Server 模式。默认为 simple
  mode: simple
  # 是否异步处理消息。默认为 false
  async: true
  batch-size: 1000
  filter: xxx
  subscribe-types:
    - ROWDATA
    - TRANSACTIONEND
    - HEARTBEAT
  timeout: 60
  unit: SECONDS
  # 是否开启消息队列。默认为 false
  # Canal 异步消费线程池配置
  thread-pool:
    # 线程池核心线程数。默认为 1
    core-pool-size: 1
    # 线程池最大线程数。默认为 1
    max-pool-size: 1
    # 线程池队列容量。默认为 1000
    queue-capacity: 1000
    # 线程池线程存活时间。默认为 60s
    keep-alive: 60s
  # Simple 客户端模式配置
  simple:
    instances:
      - # Canal Server 主机地址。如果设置了address属性，则忽略。
        host: 127.0.0.1
        # Canal Server 端口。如果设置了address属性，则忽略。默认为 5672
        port: 11111
        # Canal Server 地址。如果设置了该属性，则忽略host和port属性。
        addresses: xxx
        # Canal Server 用户名
        username: xxx
        # Canal Server 密码
        password: xxx
        # Socket 连接超时时间，单位：毫秒。默认为 60000
        so-timeout: 60000
        # Socket 空闲超时时间，单位：毫秒。默认为 3600000
        idle-timeout: 3600000
        # 重试次数;设置-1时可以subscribe阻塞等待时优雅停机
        retry-times: 3
        # 重试间隔时间，单位：毫秒。默认为 1000
        retry-interval: 1000
  # Cluster 客户端模式配置
  cluster:
    instances:
      - # Canal Server 主机地址。如果设置了address属性，则忽略。
        host: 127.0.0.1
        # Canal Server 端口。如果设置了address属性，则忽略。默认为 5672
        port: 11111
        # Canal Server 地址。如果设置了该属性，则忽略host和port属性。
        addresses: xxx
        #  Canal Zookeeper 地址
        zk-servers: : xxx
        # Canal Server 用户名
        username: xxx
        # Canal Server 密码
        password: xxx
        # Socket 连接超时时间，单位：毫秒。默认为 60000
        so-timeout: 60000
        # Socket 空闲超时时间，单位：毫秒。默认为 3600000
        idle-timeout: 3600000
        # 重试次数;设置-1时可以subscribe阻塞等待时优雅停机
        retry-times: 3
        # 重试间隔时间，单位：毫秒。默认为 1000
        retry-interval: 1000
  # Canal Kafka 消息队列配置
  kafka:
    instances:
      - # 启动时从未消费的消息位置开始
        earliest: true
        # 消息分区索引
        partition: 0
        # Kafka服务器地址
        servers: xxx,xxx,xxx
        # 订阅的消息主题
        topic: xxx
        # 消费者组ID
        group-id: xxx
        # 批量获取数据的大小
        batch-size: 64
  # Canal RabbitMQ 消息队列配置
  rabbitmq:
    instances:
      - # RabbitMQ服务器地址
        addresses: xxx,xxx,xxx
        # 虚拟主机
        vhost: xxx
        # 队列名称
        queue-name: xxx
        # 访问key
        access-key: xxx
        # 访问秘钥
        secret-key: xxx
        # 用户名
        username: xxx
        # 密码
        password: xxx
        #资源所有者的ID
        resource-owner-id: 000000
  # Canal RocketMQ 消息队列配置
  rocketmq:
    instances:
      - # RocketMQ NameServer 服务地址
        name-server: xxx
        # 订阅的消息主题
        topic: xxx
        # 消费者组名称
        group-name: xxx
        # 访问key
        access-key: xxx
        # 访问秘钥
        secret-key: xxx
        # 访问的通道
        access-channel: LOCAL
        # 是否开启消息轨迹
        enable-message-trace: false
        # 命名空间
        namespace: xxx
        # 自定义轨迹主题
        customized-trace-topic: xxx
        # 批量获取数据的大小
        batch-size: 64
  # Canal Pulsar 消息队列配置
  pulsar:
    instances:
      - # PulsarMQ 服务地址
        service-url: xxx
        # 角色认证 token
        role-token: xxx
        # 订阅的消息主题
        topic: xxx
        # 订阅客户端名称
        subscript-name: xxx
        # 每次批量获取数据的最大条目数，默认30
        batch-size: 30
        batch-timeout-seconds: 30
        batch-process-timeout-seconds: 60
        # 消费失败后的重试秒数，默认60秒
        redelivery-delay-seconds: 60
        # 当客户端接收到消息，30秒还没有返回ack给服务端时，ack超时，会重新消费该消息
        ack-timeout-seconds: 30
        # 是否开启消息失败重试功能，默认开启
        retry: true
        #  true重试(-RETRY)和死信队列(-DLQ)后缀为大写，有些地方创建的为小写，需确保正确
        retry-d-l-q-upper-case: true
        # 最大重试次数
        max-redelivery-count: 10
```

###### 2.1、@CanalEventHandler 注解事件监听消费模式

创建Java对象 CanalMessageEventHandler，适用 @CanalEventHandler 注解标注在消费者监听器上

```java
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.annotation.CanalEventHandler;
import com.alibaba.otter.canal.annotation.event.*;
import com.alibaba.otter.canal.model.CanalModel;
import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@CanalEventHandler
@Slf4j
public class CanalMessageEventHandler {

    @OnCreateTableEvent(schema = "my_auth")
    public void onCreateTableEvent(CanalModel model, CanalEntry.RowChange rowChange) {
        log.info("onCreateTableEvent");
    }

    @OnCreateIndexEvent(schema = "my_auth", table = "user_info")
    public void onCreateIndexEvent(CanalModel model, CanalEntry.RowChange rowChange) {
        log.info("OnCreateIndexEvent");
    }

    @OnInsertEvent(schema = "my_auth", table = "user_info")
    public void onEventInsertData(CanalModel model, CanalEntry.RowChange rowChange) {

        // 1，获取当前事件的操作类型
        CanalEntry.EventType eventType = rowChange.getEventType();
        // 2,获取数据集
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
        // 3,遍历RowDataList，并打印数据集
        for (CanalEntry.RowData rowData : rowDatasList) {
            JSONObject beforeData = new JSONObject();
            List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
            for (CanalEntry.Column column : beforeColumnsList) {
                beforeData.put(column.getName(), column.getValue());
            }
            JSONObject affterData = new JSONObject();
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            for (CanalEntry.Column column : afterColumnsList) {
                affterData.put(column.getName(), column.getValue());
            }

            System.out.println("Table:" + model.getTable() +
                    ",EventType:" + eventType +
                    ",Before:" + beforeData +
                    ",After:" + affterData);
        }

    }

    @OnUpdateEvent(schema = "my_auth", table = "user_info")
    public void onEventUpdateData(CanalModel model, CanalEntry.RowChange rowChange) {
        log.info("onEventUpdateData");
    }

    @OnDeleteEvent(schema = "my_auth", table = "user_info")
    public void onEventDeleteData(CanalEntry.RowChange rowChange, CanalModel model) {
        log.info("onEventDeleteData");
    }

}
```
 

###### 2.2、EntryHandler 消费者模式

创建Java对象 CanalMessageEntryHandler，实现实体处理器 EntryHandler 接口

```java
import com.alibaba.otter.canal.handler.EntryHandler;
import org.springframework.stereotype.Component;

@Component
public class CanalMessageEntryHandler implements EntryHandler<UserInfo> {

    @Override
    public void insert(UserInfo t) {
    }

    @Override
    public void update(UserInfo before, UserInfo after) {
    }

    @Override
    public void delete(UserInfo t) {
    }

}
```


## Jeebiz 技术社区

Jeebiz 技术社区 **微信公共号**、**小程序**，欢迎关注反馈意见和一起交流，关注公众号回复「Jeebiz」拉你入群。

|公共号|小程序|
|---|---|
| ![](https://raw.githubusercontent.com/hiwepy/static/main/images/qrcode_for_gh_1d965ea2dfd1_344.jpg)| ![](https://raw.githubusercontent.com/hiwepy/static/main/images/gh_09d7d00da63e_344.jpg)|
