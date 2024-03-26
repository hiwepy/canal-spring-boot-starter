package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.ClusterCanalClient;
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
import com.alibaba.otter.canal.util.ConnectorUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

@Configuration
@ConditionalOnClass({ SimpleCanalConnector.class, ClusterCanalConnector.class })
@ConditionalOnProperty(value = CanalProperties.CANAL_MODE, havingValue = "cluster")
@EnableConfigurationProperties({CanalProperties.class, CanalClusterProperties.class})
@Import(CanalThreadPoolAutoConfiguration.class)
@Slf4j
public class CanalClusterClientAutoConfiguration {

    @Bean
    public RowDataHandler<CanalEntry.RowData> rowDataHandler() {
        return new RowDataHandlerImpl(new EntryColumnModelFactory());
    }

    @Bean
    @ConditionalOnProperty(value = CanalProperties.CANAL_ASYNC, havingValue = "true", matchIfMissing = true)
    public MessageHandler messageHandler(RowDataHandler<CanalEntry.RowData> rowDataHandler,
                                         ObjectProvider<EntryHandler> entryHandlerProvider,
                                         ThreadPoolTaskExecutor threadPoolTaskExecutor) {
        return new AsyncMessageHandlerImpl(entryHandlerProvider.stream().collect(Collectors.toList()), rowDataHandler, threadPoolTaskExecutor);
    }

    @Bean
    @ConditionalOnProperty(value = CanalProperties.CANAL_ASYNC, havingValue = "false")
    public MessageHandler messageHandler(RowDataHandler<CanalEntry.RowData> rowDataHandler,
                                         ObjectProvider<EntryHandler> entryHandlerProvider) {
        return new SyncMessageHandlerImpl(entryHandlerProvider.stream().collect(Collectors.toList()), rowDataHandler);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public ClusterCanalClient clusterCanalClient(ObjectProvider<ClusterCanalConnector> connectorProvider,
                                                 ObjectProvider<MessageHandler> messageHandlerProvider,
                                                 CanalProperties canalProperties,
                                                 CanalClusterProperties connectorProperties){
        // 1. 获取Spring 上下文中所有的 ClusterCanalConnector
        List<ClusterCanalConnector> clusterCanalConnectors = connectorProvider.stream().collect(Collectors.toList());
        // 2. 初始化配置文件中配置的 SimpleCanalConnector
        if(!CollectionUtils.isEmpty(connectorProperties.getInstances())){
            clusterCanalConnectors.addAll(connectorProperties.getInstances().stream()
                    .map(instance -> ConnectorUtil.createClusterCanalConnector(instance))
                    .collect(Collectors.toList()));
        }
        // 3. 返回 ClusterCanalClient
        return (ClusterCanalClient) new ClusterCanalClient.Builder()
                .batchSize(canalProperties.getBatchSize())
                .filter(canalProperties.getFilter())
                .timeout(canalProperties.getTimeout())
                .unit(canalProperties.getUnit())
                .messageHandler(messageHandlerProvider.getIfAvailable())
                .build(clusterCanalConnectors);
    }

}
