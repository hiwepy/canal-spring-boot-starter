package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.SimpleCanalClient;
import com.alibaba.otter.canal.client.impl.ClusterCanalConnector;
import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import com.alibaba.otter.canal.factory.EntryColumnModelFactory;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.handler.MessageHandler;
import com.alibaba.otter.canal.handler.RowDataHandler;
import com.alibaba.otter.canal.handler.impl.AsyncMessageHandlerImpl;
import com.alibaba.otter.canal.handler.impl.RowDataHandlerImpl;
import com.alibaba.otter.canal.handler.impl.SyncMessageHandlerImpl;
import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.net.InetSocketAddress;
import java.util.List;

@Configuration
@ConditionalOnClass({ SimpleCanalConnector.class, ClusterCanalConnector.class })
@ConditionalOnProperty(value = CanalProperties.CANAL_MODE, havingValue = "simple", matchIfMissing = true)
@EnableConfigurationProperties({CanalProperties.class, CanalSimpleProperties.class})
@Slf4j
public class CanalSimpleClientAutoConfiguration {

    @Bean
    public RowDataHandler<CanalEntry.RowData> rowDataHandler() {
        return new RowDataHandlerImpl(new EntryColumnModelFactory());
    }

    @Bean
    @ConditionalOnProperty(value = CanalProperties.CANAL_ASYNC, havingValue = "true", matchIfMissing = true)
    public MessageHandler messageHandler(RowDataHandler<CanalEntry.RowData> rowDataHandler, List<EntryHandler> entryHandlers,
                                         ThreadPoolTaskExecutor threadPoolTaskExecutor) {
        return new AsyncMessageHandlerImpl(entryHandlers, rowDataHandler, threadPoolTaskExecutor);
    }

    @Bean
    @ConditionalOnProperty(value = CanalProperties.CANAL_ASYNC, havingValue = "false")
    public MessageHandler messageHandler(RowDataHandler<CanalEntry.RowData> rowDataHandler, List<EntryHandler> entryHandlers) {
        return new SyncMessageHandlerImpl(entryHandlers, rowDataHandler);
    }

    @Bean(initMethod = "connect", destroyMethod = "disconnect")
    public SimpleCanalConnector simpleCanalConnector(CanalSimpleProperties connectorProperties){
        InetSocketAddress address = new InetSocketAddress(connectorProperties.getHost(), connectorProperties.getPort());
        SimpleCanalConnector canalConnector = new SimpleCanalConnector(address,
                connectorProperties.getDestination(),
                connectorProperties.getUsername(),
                connectorProperties.getPassword());
        canalConnector.setSoTimeout(connectorProperties.getSoTimeout());
        canalConnector.setIdleTimeout(connectorProperties.getIdleTimeout());
        return canalConnector;
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public SimpleCanalClient simpleCanalClient(ObjectProvider<SimpleCanalConnector> connectorProvider,
                                               ObjectProvider<MessageHandler> messageHandlerProvider,
                                               CanalProperties canalProperties){
        return (SimpleCanalClient) new SimpleCanalClient.Builder()
                .batchSize(canalProperties.getBatchSize())
                .filter(canalProperties.getFilter())
                .timeout(canalProperties.getTimeout())
                .unit(canalProperties.getUnit())
                .messageHandler(messageHandlerProvider.getIfAvailable())
                .build(connectorProvider.getIfAvailable());
    }

}
