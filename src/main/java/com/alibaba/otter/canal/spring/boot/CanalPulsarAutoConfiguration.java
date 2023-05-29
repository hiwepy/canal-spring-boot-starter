package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.pulsarmq.PulsarMQCanalConnector;
import com.alibaba.otter.canal.spring.boot.hooks.CanalShutdownHook;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass({ PulsarMQCanalConnector.class, PulsarClient.class })
@ConditionalOnProperty(prefix = CanalPulsarProperties.PREFIX, value = "enabled", havingValue = "true")
@EnableConfigurationProperties(CanalPulsarProperties.class)
@Slf4j
public class CanalPulsarAutoConfiguration {

    @Bean(initMethod = "connect", destroyMethod = "disconnect")
    public PulsarMQCanalConnector defaultPulsarMQCanalConnector(CanalPulsarProperties properties) {
        PulsarMQCanalConnector connector = new PulsarMQCanalConnector(properties.isFlatMessage(),
                properties.getServiceUrl(), properties.getRoleToken(), properties.getTopic(),
                properties.getSubscriptName(), properties.getBatchSize(), properties.getBatchTimeoutSeconds(),
                properties.getBatchProcessTimeoutSeconds(), properties.getRedeliveryDelaySeconds(),
                properties.getAckTimeoutSeconds(),
                properties.isRetry(), properties.isRetryDLQUpperCase(), properties.getMaxRedeliveryCount());
        Runtime.getRuntime().addShutdownHook(new CanalShutdownHook(connector));
        return connector;
    }

}
