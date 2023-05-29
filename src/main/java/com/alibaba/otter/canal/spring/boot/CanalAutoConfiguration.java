package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConnectorConsumer;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConsumeMessageService;
import com.alibaba.otter.canal.spring.boot.consumer.impl.CanalConnectorConsumerImpl;
import com.alibaba.otter.canal.spring.boot.consumer.impl.CanalMQConnectorConsumerImpl;
import com.alibaba.otter.canal.spring.boot.consumer.listener.MessageListenerConcurrently;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.stream.Collectors;

@Configuration
@ConditionalOnClass({ CanalConnector.class, CanalLifeCycle.class, CanalPacket.class })
@ConditionalOnProperty(prefix = CanalProperties.PREFIX, value = "enabled", havingValue = "true")
@EnableConfigurationProperties(CanalProperties.class)
@Slf4j
public class CanalAutoConfiguration {

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    @ConditionalOnBean(MessageListenerConcurrently.class)
    public CanalConnectorConsumer canalConnectorConsumer(
            ObjectProvider<CanalConnector> canalConnectorProvider,
            ObjectProvider<CanalConsumeMessageService> consumeMessageServiceProvider){

        List<CanalConnector> connectors = canalConnectorProvider.stream()
                .filter(connector -> !CanalMQConnector.class.isAssignableFrom(connector.getClass()))
                .collect(Collectors.toList());

        return new CanalConnectorConsumerImpl(connectors, consumeMessageServiceProvider.getIfAvailable());
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    @ConditionalOnBean(MessageListenerConcurrently.class)
    public CanalMQConnectorConsumerImpl canalMQCanalConnectorConsumer(
            ObjectProvider<CanalMQConnector> rocketMQCanalConnectorProvider,
            ObjectProvider<CanalConsumeMessageService> consumeMessageServiceProvider){

        List<CanalMQConnector> connectors = rocketMQCanalConnectorProvider.stream()
                .filter(connector -> CanalMQConnector.class.isAssignableFrom(connector.getClass()))
                .collect(Collectors.toList());

        return new CanalMQConnectorConsumerImpl(connectors, consumeMessageServiceProvider.getIfAvailable());
    }





}
