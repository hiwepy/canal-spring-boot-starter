package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.PulsarMQCanalClient;
import com.alibaba.otter.canal.client.pulsarmq.PulsarMQCanalConnector;
import com.alibaba.otter.canal.factory.MapColumnModelFactory;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.handler.MessageHandler;
import com.alibaba.otter.canal.handler.RowDataHandler;
import com.alibaba.otter.canal.handler.impl.AsyncFlatMessageHandlerImpl;
import com.alibaba.otter.canal.handler.impl.MapRowDataHandlerImpl;
import com.alibaba.otter.canal.handler.impl.SyncFlatMessageHandlerImpl;
import com.alibaba.otter.canal.util.ConnectorUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
@ConditionalOnClass({ PulsarMQCanalConnector.class, PulsarClient.class })
@ConditionalOnProperty(value = CanalProperties.CANAL_MODE, havingValue = "pulsarmq")
@EnableConfigurationProperties({CanalProperties.class, CanalPulsarClientProperties.class})
@Import(CanalThreadPoolAutoConfiguration.class)
@Slf4j
public class CanalPulsarClientAutoConfiguration {

    @Bean
    public RowDataHandler<List<Map<String, String>>> rowDataHandler() {
        return new MapRowDataHandlerImpl(new MapColumnModelFactory());
    }

    @Bean
    @ConditionalOnProperty(value = CanalProperties.CANAL_ASYNC, havingValue = "true", matchIfMissing = true)
    public MessageHandler messageHandler(RowDataHandler<List<Map<String, String>>> rowDataHandler,
                                         ObjectProvider<EntryHandler> entryHandlerProvider,
                                         @Qualifier("canalTaskExecutor") ThreadPoolTaskExecutor canalTaskExecutor) {
        return new AsyncFlatMessageHandlerImpl(entryHandlerProvider.stream().collect(Collectors.toList()), rowDataHandler, canalTaskExecutor);
    }

    @Bean
    @ConditionalOnProperty(value = CanalProperties.CANAL_ASYNC, havingValue = "false")
    public MessageHandler messageHandler(RowDataHandler<List<Map<String, String>>> rowDataHandler,
                                         ObjectProvider<EntryHandler> entryHandlerProvider) {
        return new SyncFlatMessageHandlerImpl(entryHandlerProvider.stream().collect(Collectors.toList()), rowDataHandler);
    }

    @Bean
    public List<PulsarMQCanalConnector> pulsarMQCanalConnectors(CanalPulsarClientProperties connectorProperties){
        Assert.notEmpty(connectorProperties.getInstances(), "No pulsarMQ canal instance configured");
        return connectorProperties.getInstances().stream()
                .map(instance -> ConnectorUtil.createPulsarMQCanalConnector(instance))
                .collect(Collectors.toList());
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public PulsarMQCanalClient kafkaCanalClient(ObjectProvider<PulsarMQCanalConnector> connectorProvider,
                                                ObjectProvider<MessageHandler> messageHandlerProvider,
                                                CanalProperties canalProperties){
        return (PulsarMQCanalClient) new PulsarMQCanalClient.Builder()
                .batchSize(canalProperties.getBatchSize())
                .filter(canalProperties.getFilter())
                .timeout(canalProperties.getTimeout())
                .unit(canalProperties.getUnit())
                .messageHandler(messageHandlerProvider.getIfAvailable())
                .build(connectorProvider.stream().collect(Collectors.toList()));
    }

}
