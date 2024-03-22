package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.ClusterCanalClient;
import com.alibaba.otter.canal.client.impl.ClusterCanalConnector;
import com.alibaba.otter.canal.client.impl.ClusterNodeAccessStrategy;
import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import com.alibaba.otter.canal.client.impl.SimpleNodeAccessStrategy;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.factory.EntryColumnModelFactory;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.handler.MessageHandler;
import com.alibaba.otter.canal.handler.RowDataHandler;
import com.alibaba.otter.canal.handler.impl.AsyncMessageHandlerImpl;
import com.alibaba.otter.canal.handler.impl.RowDataHandlerImpl;
import com.alibaba.otter.canal.handler.impl.SyncMessageHandlerImpl;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.util.AddressUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.StringUtils;

import java.util.List;

@Configuration
@ConditionalOnClass({ SimpleCanalConnector.class, ClusterCanalConnector.class })
@ConditionalOnProperty(prefix = CanalProperties.CANAL_MODE, havingValue = "cluster")
@EnableConfigurationProperties({CanalProperties.class, CanalSimpleProperties.class})
@Slf4j
public class CanalClusterClientAutoConfiguration {

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
    public ClusterCanalConnector clusterCanalConnector(CanalSimpleProperties connectorProperties){
        if (StringUtils.hasText(connectorProperties.getZkServers())) {
            ClusterCanalConnector canalConnector = new ClusterCanalConnector(connectorProperties.getUsername(),
                    connectorProperties.getPassword(),
                    connectorProperties.getDestination(),
                    new ClusterNodeAccessStrategy(connectorProperties.getDestination(),
                            ZkClientx.getZkClient(connectorProperties.getZkServers())));
            canalConnector.setSoTimeout(connectorProperties.getSoTimeout());
            canalConnector.setIdleTimeout(connectorProperties.getIdleTimeout());
            canalConnector.setRetryTimes(connectorProperties.getRetryTimes());
            canalConnector.setRetryInterval(connectorProperties.getRetryInterval());
            return canalConnector;
        }
        ClusterCanalConnector canalConnector = new ClusterCanalConnector(
                connectorProperties.getUsername(),
                connectorProperties.getPassword(),
                connectorProperties.getDestination(),
                new SimpleNodeAccessStrategy(AddressUtils.parseAddresses(connectorProperties.getAddresses())));
        canalConnector.setSoTimeout(connectorProperties.getSoTimeout());
        canalConnector.setIdleTimeout(connectorProperties.getIdleTimeout());
        canalConnector.setRetryTimes(connectorProperties.getRetryTimes());
        canalConnector.setRetryInterval(connectorProperties.getRetryInterval());
        return canalConnector;
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public ClusterCanalClient clusterCanalClient(ObjectProvider<ClusterCanalConnector> connectorProvider,
                                                ObjectProvider<MessageHandler> messageHandlerProvider,
                                                CanalProperties canalProperties){
        return (ClusterCanalClient) new ClusterCanalClient.Builder()
                .batchSize(canalProperties.getBatchSize())
                .filter(canalProperties.getFilter())
                .timeout(canalProperties.getTimeout())
                .unit(canalProperties.getUnit())
                .messageHandler(messageHandlerProvider.getIfAvailable())
                .build(connectorProvider.getIfAvailable());
    }

}
