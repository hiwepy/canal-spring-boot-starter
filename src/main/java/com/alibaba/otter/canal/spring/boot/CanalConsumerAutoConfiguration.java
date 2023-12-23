package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConsumeMessageService;
import com.alibaba.otter.canal.spring.boot.consumer.impl.CanalConnectorConsumerImpl;
import com.alibaba.otter.canal.spring.boot.consumer.impl.CanalMQConnectorConsumerImpl;
import com.alibaba.otter.canal.spring.boot.consumer.impl.ConsumeMessageConcurrentlyServiceImpl;
import com.alibaba.otter.canal.spring.boot.consumer.listener.MessageListenerConcurrently;
import com.alibaba.otter.canal.spring.boot.hooks.CanalConsumerHook;
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
@ConditionalOnProperty(prefix = CanalProperties.PREFIX, value = "consumer-mode", havingValue = "THREAD_POOL")
@EnableConfigurationProperties({CanalProperties.class, CanalConsumerProperties.class})
@Slf4j
public class CanalConsumerAutoConfiguration {

    @Bean(initMethod = "start")
    public CanalConsumeMessageService canalConsumeMessageService(
            CanalConsumerProperties consumerProperties,
            ObjectProvider<MessageListenerConcurrently> messageListenerProvider){
        CanalConsumeMessageService consumeMessageService = new ConsumeMessageConcurrentlyServiceImpl(consumerProperties, messageListenerProvider.getIfAvailable());
        Runtime.getRuntime().addShutdownHook(new CanalConsumerHook(consumeMessageService, consumerProperties.getAwaitTerminateMillis()));
        return consumeMessageService;
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    @ConditionalOnBean(MessageListenerConcurrently.class)
    public CanalConnectorConsumerImpl canalConnectorConsumer(
            CanalConsumerProperties consumerProperties,
            ObjectProvider<CanalConnector> canalConnectorProvider,
            ObjectProvider<CanalConsumeMessageService> consumeMessageServiceProvider){

        List<CanalConnector> connectors = canalConnectorProvider.stream()
                .filter(connector -> !CanalMQConnector.class.isAssignableFrom(connector.getClass()))
                .collect(Collectors.toList());

        CanalConnectorConsumerImpl consumerImpl = new CanalConnectorConsumerImpl(connectors, consumeMessageServiceProvider.getIfAvailable());
        consumerImpl.init(consumerProperties);
        return consumerImpl;
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    @ConditionalOnBean(MessageListenerConcurrently.class)
    public CanalMQConnectorConsumerImpl canalMQCanalConnectorConsumer(
            CanalConsumerProperties consumerProperties,
            ObjectProvider<CanalMQConnector> rocketMQCanalConnectorProvider,
            ObjectProvider<CanalConsumeMessageService> consumeMessageServiceProvider){

        List<CanalMQConnector> connectors = rocketMQCanalConnectorProvider.stream()
                .filter(connector -> CanalMQConnector.class.isAssignableFrom(connector.getClass()))
                .collect(Collectors.toList());

        CanalMQConnectorConsumerImpl consumerImpl = new CanalMQConnectorConsumerImpl(connectors, consumeMessageServiceProvider.getIfAvailable());
        consumerImpl.init(consumerProperties);
        return consumerImpl;

    }





}
