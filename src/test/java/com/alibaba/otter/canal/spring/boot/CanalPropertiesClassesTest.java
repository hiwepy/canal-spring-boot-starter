package com.alibaba.otter.canal.spring.boot;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CanalPropertiesClassesTest {

    @Test
    void simplePropertiesDefaults() {
        CanalSimpleProperties props = new CanalSimpleProperties();
        assertThat(props.getInstances()).isNotNull().isEmpty();
        assertThat(CanalSimpleProperties.PREFIX).isEqualTo("canal.simple");
    }

    @Test
    void simplePropertiesSetters() {
        CanalSimpleProperties props = new CanalSimpleProperties();
        CanalSimpleProperties.Instance inst = new CanalSimpleProperties.Instance();
        props.setInstances(java.util.List.of(inst));
        assertThat(props.getInstances()).hasSize(1);
    }

    @Test
    void simpleInstanceGettersSetters() {
        CanalSimpleProperties.Instance inst = new CanalSimpleProperties.Instance();
        inst.setHost("localhost");
        inst.setPort(11111);
        inst.setDestination("dest");
        inst.setUsername("user");
        inst.setPassword("pass");
        inst.setSoTimeout(5000);
        inst.setIdleTimeout(10000);
        inst.setRetryTimes(3);
        inst.setRetryInterval(1000);
        assertThat(inst.getHost()).isEqualTo("localhost");
        assertThat(inst.getPort()).isEqualTo(11111);
        assertThat(inst.getDestination()).isEqualTo("dest");
        assertThat(inst.getUsername()).isEqualTo("user");
        assertThat(inst.getPassword()).isEqualTo("pass");
        assertThat(inst.getSoTimeout()).isEqualTo(5000);
        assertThat(inst.getIdleTimeout()).isEqualTo(10000);
        assertThat(inst.getRetryTimes()).isEqualTo(3);
        assertThat(inst.getRetryInterval()).isEqualTo(1000);
    }

    @Test
    void clusterPropertiesDefaults() {
        CanalClusterProperties props = new CanalClusterProperties();
        assertThat(props.getInstances()).isNotNull().isEmpty();
        assertThat(CanalClusterProperties.PREFIX).isEqualTo("canal.cluster");
    }

    @Test
    void clusterInstanceGettersSetters() {
        CanalClusterProperties.Instance inst = new CanalClusterProperties.Instance();
        inst.setAddresses("addr");
        inst.setZkServers("zk");
        inst.setDestination("dest");
        inst.setUsername("user");
        inst.setPassword("pass");
        assertThat(inst.getAddresses()).isEqualTo("addr");
        assertThat(inst.getZkServers()).isEqualTo("zk");
        assertThat(inst.getDestination()).isEqualTo("dest");
        assertThat(inst.getUsername()).isEqualTo("user");
        assertThat(inst.getPassword()).isEqualTo("pass");
    }

    @Test
    void kafkaPropertiesDefaults() {
        CanalKafkaClientProperties props = new CanalKafkaClientProperties();
        assertThat(props.isEnabled()).isFalse();
        assertThat(props.getInstances()).isNotNull().isEmpty();
        assertThat(CanalKafkaClientProperties.PREFIX).isEqualTo("canal.kafka");
    }

    @Test
    void kafkaPropertiesSetters() {
        CanalKafkaClientProperties props = new CanalKafkaClientProperties();
        props.setEnabled(true);
        assertThat(props.isEnabled()).isTrue();
        CanalKafkaClientProperties.Instance inst = new CanalKafkaClientProperties.Instance();
        props.setInstances(java.util.List.of(inst));
        assertThat(props.getInstances()).hasSize(1);
    }

    @Test
    void kafkaInstanceGettersSetters() {
        CanalKafkaClientProperties.Instance inst = new CanalKafkaClientProperties.Instance();
        inst.setServers("broker:9092");
        inst.setTopic("topic");
        inst.setGroupId("group");
        inst.setBatchSize(100);
        inst.setFlatMessage(true);
        inst.setEarliest(false);
        inst.setPartition(0);
        assertThat(inst.getServers()).isEqualTo("broker:9092");
        assertThat(inst.getTopic()).isEqualTo("topic");
        assertThat(inst.getGroupId()).isEqualTo("group");
        assertThat(inst.getBatchSize()).isEqualTo(100);
        assertThat(inst.isFlatMessage()).isTrue();
        assertThat(inst.isEarliest()).isFalse();
        assertThat(inst.getPartition()).isEqualTo(0);
    }

    @Test
    void pulsarPropertiesDefaults() {
        CanalPulsarClientProperties props = new CanalPulsarClientProperties();
        assertThat(props.isEnabled()).isFalse();
        assertThat(props.getInstances()).isNotNull().isEmpty();
        assertThat(CanalPulsarClientProperties.PREFIX).isEqualTo("canal.pulsar");
    }

    @Test
    void pulsarPropertiesSetters() {
        CanalPulsarClientProperties props = new CanalPulsarClientProperties();
        props.setEnabled(true);
        assertThat(props.isEnabled()).isTrue();
        CanalPulsarClientProperties.Instance inst = new CanalPulsarClientProperties.Instance();
        props.setInstances(java.util.List.of(inst));
        assertThat(props.getInstances()).hasSize(1);
    }

    @Test
    void pulsarInstanceGettersSetters() {
        CanalPulsarClientProperties.Instance inst = new CanalPulsarClientProperties.Instance();
        inst.setServiceUrl("pulsar://localhost");
        inst.setRoleToken("token");
        inst.setTopic("topic");
        inst.setSubscriptName("sub");
        inst.setBatchSize(50);
        inst.setBatchTimeoutSeconds(30);
        inst.setBatchProcessTimeoutSeconds(60);
        inst.setRedeliveryDelaySeconds(10);
        inst.setAckTimeoutSeconds(20);
        inst.setRetry(true);
        inst.setRetryDLQUpperCase(false);
        inst.setMaxRedeliveryCount(100);
        inst.setFlatMessage(true);
        assertThat(inst.getServiceUrl()).isEqualTo("pulsar://localhost");
        assertThat(inst.getRoleToken()).isEqualTo("token");
        assertThat(inst.getTopic()).isEqualTo("topic");
        assertThat(inst.getSubscriptName()).isEqualTo("sub");
        assertThat(inst.getBatchSize()).isEqualTo(50);
        assertThat(inst.getBatchTimeoutSeconds()).isEqualTo(30);
        assertThat(inst.getBatchProcessTimeoutSeconds()).isEqualTo(60);
        assertThat(inst.getRedeliveryDelaySeconds()).isEqualTo(10);
        assertThat(inst.getAckTimeoutSeconds()).isEqualTo(20);
        assertThat(inst.isRetry()).isTrue();
        assertThat(inst.isRetryDLQUpperCase()).isFalse();
        assertThat(inst.getMaxRedeliveryCount()).isEqualTo(100);
        assertThat(inst.isFlatMessage()).isTrue();
    }

    @Test
    void rocketmqPropertiesDefaults() {
        CanalRocketmqClientProperties props = new CanalRocketmqClientProperties();
        assertThat(props.isEnabled()).isFalse();
        assertThat(props.getInstances()).isNotNull().isEmpty();
        assertThat(CanalRocketmqClientProperties.PREFIX).isEqualTo("canal.rocketmq");
    }

    @Test
    void rocketmqPropertiesSetters() {
        CanalRocketmqClientProperties props = new CanalRocketmqClientProperties();
        props.setEnabled(true);
        assertThat(props.isEnabled()).isTrue();
        CanalRocketmqClientProperties.Instance inst = new CanalRocketmqClientProperties.Instance();
        props.setInstances(java.util.List.of(inst));
        assertThat(props.getInstances()).hasSize(1);
    }

    @Test
    void rocketmqInstanceGettersSetters() {
        CanalRocketmqClientProperties.Instance inst = new CanalRocketmqClientProperties.Instance();
        inst.setNameServer("ns:9876");
        inst.setTopic("topic");
        inst.setGroupName("group");
        inst.setEnableMessageTrace(true);
        inst.setAccessKey("ak");
        inst.setSecretKey("sk");
        inst.setAccessChannel("cloud");
        inst.setNamespace("ns");
        inst.setCustomizedTraceTopic("trace");
        inst.setBatchSize(200);
        assertThat(inst.getNameServer()).isEqualTo("ns:9876");
        assertThat(inst.getTopic()).isEqualTo("topic");
        assertThat(inst.getGroupName()).isEqualTo("group");
        assertThat(inst.isEnableMessageTrace()).isTrue();
        assertThat(inst.getAccessKey()).isEqualTo("ak");
        assertThat(inst.getSecretKey()).isEqualTo("sk");
        assertThat(inst.getAccessChannel()).isEqualTo("cloud");
        assertThat(inst.getNamespace()).isEqualTo("ns");
        assertThat(inst.getCustomizedTraceTopic()).isEqualTo("trace");
        assertThat(inst.getBatchSize()).isEqualTo(200);
    }

    @Test
    void rabbitmqPropertiesDefaults() {
        CanalRabbitmqClientProperties props = new CanalRabbitmqClientProperties();
        assertThat(props.isEnabled()).isFalse();
        assertThat(props.getInstances()).isNotNull().isEmpty();
        assertThat(CanalRabbitmqClientProperties.PREFIX).isEqualTo("canal.rabbitmq");
    }

    @Test
    void rabbitmqPropertiesSetters() {
        CanalRabbitmqClientProperties props = new CanalRabbitmqClientProperties();
        props.setEnabled(true);
        assertThat(props.isEnabled()).isTrue();
        CanalRabbitmqClientProperties.Instance inst = new CanalRabbitmqClientProperties.Instance();
        props.setInstances(java.util.List.of(inst));
        assertThat(props.getInstances()).hasSize(1);
    }

    @Test
    void rabbitmqInstanceGettersSetters() {
        CanalRabbitmqClientProperties.Instance inst = new CanalRabbitmqClientProperties.Instance();
        inst.setAddresses("addr");
        inst.setVhost("/");
        inst.setQueueName("q");
        inst.setAccessKey("ak");
        inst.setSecretKey("sk");
        inst.setResourceOwnerId(123L);
        inst.setUsername("user");
        inst.setPassword("pass");
        inst.setFlatMessage(true);
        assertThat(inst.getAddresses()).isEqualTo("addr");
        assertThat(inst.getVhost()).isEqualTo("/");
        assertThat(inst.getQueueName()).isEqualTo("q");
        assertThat(inst.getAccessKey()).isEqualTo("ak");
        assertThat(inst.getSecretKey()).isEqualTo("sk");
        assertThat(inst.getResourceOwnerId()).isEqualTo(123L);
        assertThat(inst.getUsername()).isEqualTo("user");
        assertThat(inst.getPassword()).isEqualTo("pass");
        assertThat(inst.isFlatMessage()).isTrue();
    }

    @Test
    void threadPoolPropertiesDefaults() {
        CanalThreadPoolProperties props = new CanalThreadPoolProperties();
        assertThat(props.getCorePoolSize()).isEqualTo(1);
        assertThat(props.getMaxPoolSize()).isGreaterThan(0);
        assertThat(props.getQueueCapacity()).isEqualTo(Integer.MAX_VALUE);
        assertThat(props.isDaemon()).isFalse();
        assertThat(props.getThreadNamePrefix()).isNotEmpty();
        assertThat(props.getRejectedPolicy()).isEqualTo(CanalThreadPoolProperties.RejectedPolicy.AbortPolicy);
        assertThat(CanalThreadPoolProperties.PREFIX).isEqualTo("canal.thread-pool");
    }

    @Test
    void threadPoolSettersGetters() {
        CanalThreadPoolProperties props = new CanalThreadPoolProperties();
        props.setCorePoolSize(5);
        props.setMaxPoolSize(10);
        props.setQueueCapacity(100);
        props.setDaemon(true);
        props.setThreadNamePrefix("test-");
        props.setRejectedPolicy(CanalThreadPoolProperties.RejectedPolicy.CallerRunsPolicy);
        assertThat(props.getCorePoolSize()).isEqualTo(5);
        assertThat(props.getMaxPoolSize()).isEqualTo(10);
        assertThat(props.getQueueCapacity()).isEqualTo(100);
        assertThat(props.isDaemon()).isTrue();
        assertThat(props.getThreadNamePrefix()).isEqualTo("test-");
        assertThat(props.getRejectedPolicy()).isEqualTo(CanalThreadPoolProperties.RejectedPolicy.CallerRunsPolicy);
    }

    @Test
    void rejectedPolicyEnum() {
        assertThat(CanalThreadPoolProperties.RejectedPolicy.values()).hasSize(4);
        for (CanalThreadPoolProperties.RejectedPolicy policy : CanalThreadPoolProperties.RejectedPolicy.values()) {
            assertThat(policy.getRejectedExecutionHandler()).isNotNull();
        }
    }

    @Test
    void threadPoolKeepAliveAndShutdown() {
        CanalThreadPoolProperties props = new CanalThreadPoolProperties();
        props.setKeepAlive(java.time.Duration.ofSeconds(120));
        props.setAllowCoreThreadTimeOut(true);
        props.setWaitForTasksToCompleteOnShutdown(true);
        props.setAwaitTerminationSeconds(30);
        assertThat(props.getKeepAlive()).isEqualTo(java.time.Duration.ofSeconds(120));
        assertThat(props.isAllowCoreThreadTimeOut()).isTrue();
        assertThat(props.isWaitForTasksToCompleteOnShutdown()).isTrue();
        assertThat(props.getAwaitTerminationSeconds()).isEqualTo(30);
    }

    @Test
    void threadPoolRejectedPolicies() {
        CanalThreadPoolProperties props = new CanalThreadPoolProperties();
        props.setRejectedPolicy(CanalThreadPoolProperties.RejectedPolicy.DiscardPolicy);
        assertThat(props.getRejectedPolicy()).isEqualTo(CanalThreadPoolProperties.RejectedPolicy.DiscardPolicy);
        assertThat(props.getRejectedPolicy().getRejectedExecutionHandler()).isNotNull();

        props.setRejectedPolicy(CanalThreadPoolProperties.RejectedPolicy.DiscardOldestPolicy);
        assertThat(props.getRejectedPolicy()).isEqualTo(CanalThreadPoolProperties.RejectedPolicy.DiscardOldestPolicy);
        assertThat(props.getRejectedPolicy().getRejectedExecutionHandler()).isNotNull();
    }

    @Test
    void clusterInstanceTimeoutAndRetry() {
        CanalClusterProperties.Instance inst = new CanalClusterProperties.Instance();
        inst.setSoTimeout(10000);
        inst.setIdleTimeout(20000);
        inst.setRetryTimes(5);
        inst.setRetryInterval(3000);
        assertThat(inst.getSoTimeout()).isEqualTo(10000);
        assertThat(inst.getIdleTimeout()).isEqualTo(20000);
        assertThat(inst.getRetryTimes()).isEqualTo(5);
        assertThat(inst.getRetryInterval()).isEqualTo(3000);
    }

    @Test
    void simpleInstanceTimeoutAndRetry() {
        CanalSimpleProperties.Instance inst = new CanalSimpleProperties.Instance();
        inst.setSoTimeout(10000);
        inst.setIdleTimeout(20000);
        inst.setRetryTimes(5);
        inst.setRetryInterval(3000);
        assertThat(inst.getSoTimeout()).isEqualTo(10000);
        assertThat(inst.getIdleTimeout()).isEqualTo(20000);
        assertThat(inst.getRetryTimes()).isEqualTo(5);
        assertThat(inst.getRetryInterval()).isEqualTo(3000);
    }
}
