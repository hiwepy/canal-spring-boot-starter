package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.KafkaCanalClient;
import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import com.alibaba.otter.canal.client.kafka.KafkaOffsetCanalConnector;
import com.alibaba.otter.canal.factory.MapColumnModelFactory;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.handler.MessageHandler;
import com.alibaba.otter.canal.handler.RowDataHandler;
import com.alibaba.otter.canal.handler.impl.AsyncFlatMessageHandlerImpl;
import com.alibaba.otter.canal.handler.impl.MapRowDataHandlerImpl;
import com.alibaba.otter.canal.handler.impl.SyncFlatMessageHandlerImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
@ConditionalOnClass({ KafkaCanalConnector.class, KafkaConsumer.class })
@ConditionalOnProperty(prefix = CanalProperties.CANAL_MODE, havingValue = "kafka")
@EnableConfigurationProperties(CanalKafkaClientProperties.class)
@Slf4j
public class CanalKafkaClientAutoConfiguration {

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

    @Bean(initMethod = "connect", destroyMethod = "disconnect")
    @ConditionalOnBean(KafkaCanalConnector.class)
    public KafkaCanalConnector kafkaCanalConnector(CanalKafkaClientProperties properties) {
        KafkaCanalConnector connector = properties.isEarliest() ? new KafkaOffsetCanalConnector(properties.getServers(),
                properties.getTopic(),  properties.getPartition(), properties.getGroupId(),
                properties.isFlatMessage()) : new KafkaCanalConnector(properties.getServers(),
                properties.getTopic(),  properties.getPartition(), properties.getGroupId(),
                properties.getBatchSize(), properties.isFlatMessage());
        return connector;
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public KafkaCanalClient kafkaCanalClient(ObjectProvider<KafkaCanalConnector> connectorProvider,
                                             ObjectProvider<MessageHandler> messageHandlerProvider,
                                             CanalProperties canalProperties){
        return KafkaCanalClient.Builder.builder()
                .batchSize(canalProperties.getBatchSize())
                .filter(canalProperties.getFilter())
                .subscribeTypes(canalProperties.getSubscribeTypes())
                .timeout(canalProperties.getTimeout())
                .unit(canalProperties.getUnit())
                .messageHandler(messageHandlerProvider.getIfAvailable())
                .build(connectorProvider.getIfAvailable());
    }

}
