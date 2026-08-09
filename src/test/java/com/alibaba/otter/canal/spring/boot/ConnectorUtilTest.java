package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.util.ConnectorUtil;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectorUtilTest {

    @Test
    void createSimpleCanalConnector() {
        CanalSimpleProperties.Instance inst = new CanalSimpleProperties.Instance();
        inst.setHost("127.0.0.1");
        inst.setPort(11111);
        inst.setDestination("test");
        var connector = ConnectorUtil.createSimpleCanalConnector(inst);
        assertThat(connector).isNotNull();
    }

    @Test
    void createClusterCanalConnector() {
        CanalClusterProperties.Instance inst = new CanalClusterProperties.Instance();
        inst.setAddresses("127.0.0.1:11111");
        inst.setDestination("test");
        var connector = ConnectorUtil.createClusterCanalConnector(inst);
        assertThat(connector).isNotNull();
    }

    @Test
    void createKafkaCanalConnector() {
        CanalKafkaClientProperties.Instance inst = new CanalKafkaClientProperties.Instance();
        inst.setServers("localhost:9092");
        inst.setTopic("test");
        inst.setGroupId("group");
        var connector = ConnectorUtil.createKafkaCanalConnector(inst);
        assertThat(connector).isNotNull();
    }

    @Test
    void createRocketMQCanalConnector() {
        CanalRocketmqClientProperties.Instance inst = new CanalRocketmqClientProperties.Instance();
        inst.setNameServer("localhost:9876");
        inst.setTopic("test");
        inst.setGroupName("group");
        var connector = ConnectorUtil.createRocketMQCanalConnector(inst);
        assertThat(connector).isNotNull();
    }

    @Test
    void createPulsarMQCanalConnector() {
        CanalPulsarClientProperties.Instance inst = new CanalPulsarClientProperties.Instance();
        inst.setServiceUrl("pulsar://localhost:6650");
        inst.setTopic("test");
        inst.setSubscriptName("sub");
        var connector = ConnectorUtil.createPulsarMQCanalConnector(inst);
        assertThat(connector).isNotNull();
    }

    @Test
    void createRabbitMQCanalConnector() {
        CanalRabbitmqClientProperties.Instance inst = new CanalRabbitmqClientProperties.Instance();
        inst.setAddresses("localhost:5672");
        inst.setQueueName("test");
        var connector = ConnectorUtil.createRabbitMQCanalConnector(inst);
        assertThat(connector).isNotNull();
    }
}
