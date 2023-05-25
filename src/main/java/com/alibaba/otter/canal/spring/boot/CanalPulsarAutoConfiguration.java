package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.pulsarmq.PulsarMQCanalConnector;
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
    public PulsarMQCanalConnector pulsarMQCanalConnector(CanalPulsarProperties properties) {
        final PulsarMQCanalConnector connector = new PulsarMQCanalConnector(properties.isFlatMessage(),
                properties.getServiceUrl(), properties.getRoleToken(), properties.getTopic(),
                properties.getSubscriptName(), properties.getBatchSize(), properties.getBatchTimeoutSeconds(),
                properties.getBatchProcessTimeoutSeconds(), properties.getRedeliveryDelaySeconds(), properties.getAckTimeoutSeconds(),
                properties.isRetry(), properties.isRetryDLQUpperCase(), properties.getMaxRedeliveryCount());
        try {


        } catch (Throwable e) {
            log.error("## Something going wrong when starting up the rocketmq consumer:", e);
            System.exit(0);
        }

        return connector;
    }

	
}
