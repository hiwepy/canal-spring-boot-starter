package com.alibaba.otter.canal.spring.boot;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.client.ClusterCanalClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.client.impl.ClusterCanalConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.factory.EntryColumnModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.handler.EntryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.handler.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.handler.RowDataHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.handler.impl.AsyncMessageHandlerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.handler.impl.RowDataHandlerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.handler.impl.SyncMessageHandlerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.util.ConnectorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Import;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.stream.Collectors;

/**
 * Spring Boot auto-configuration for the Canal <strong>cluster</strong> client.
 * <p>
 * Activated when {@code canal.mode=cluster} and wires one or more
 * {@code ClusterCanalConnector} instances (with optional ZooKeeper failover) to
 * a {@link ClusterCanalClient}. Registers a {@link RowDataHandler}, an async or
 * sync {@link MessageHandler} based on {@code canal.async}, and the
 * Spring-managed client bean.
 * </p>
 *
 * <h3>Configuration keys</h3>
 * <ul>
 *   <li>{@code canal.mode} — must be {@code cluster}</li>
 *   <li>{@code canal.async} — whether message dispatch is asynchronous (default {@code true})</li>
 *   <li>{@code canal.cluster.instances} — list of Canal cluster connection definitions</li>
 * </ul>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@Configuration
@ConditionalOnClass({ SimpleCanalConnector.class, ClusterCanalConnector.class })
@ConditionalOnProperty(value = CanalProperties.CANAL_MODE, havingValue = "cluster")
@EnableConfigurationProperties({CanalProperties.class, CanalClusterProperties.class})
@Import(CanalThreadPoolAutoConfiguration.class)
public class CanalClusterClientAutoConfiguration {

    /**
     * Creates the {@link RowDataHandler} that converts Canal row data into entry model objects.
     *
     * @return a row data handler backed by the {@link EntryColumnModelFactory}
     */
    @Bean
    public RowDataHandler<CanalEntry.RowData> rowDataHandler() {
        return new RowDataHandlerImpl(new EntryColumnModelFactory());
    }

    /**
     * Creates the asynchronous {@link MessageHandler} used when {@code canal.async=true}.
     *
     * @param properties            common Canal properties providing the entry types to subscribe to
     * @param rowDataHandler        the row data handler used to transform messages
     * @param entryHandlerProvider  provider of user-defined {@link EntryHandler} beans
     * @param threadPoolTaskExecutor the Canal task executor used for async dispatch
     * @return an asynchronous message handler implementation
     */
    @Bean
    @ConditionalOnProperty(value = CanalProperties.CANAL_ASYNC, havingValue = "true", matchIfMissing = true)
    public MessageHandler asyncMessageHandler(CanalProperties properties,
                                         RowDataHandler<CanalEntry.RowData> rowDataHandler,
                                         ObjectProvider<EntryHandler> entryHandlerProvider,
                                         ThreadPoolTaskExecutor threadPoolTaskExecutor) {
        return new AsyncMessageHandlerImpl(properties.getSubscribeTypes(), entryHandlerProvider.stream().collect(Collectors.toList()), rowDataHandler, threadPoolTaskExecutor);
    }

    /**
     * Creates the synchronous {@link MessageHandler} used when {@code canal.async=false}.
     *
     * @param properties           common Canal properties providing the entry types to subscribe to
     * @param rowDataHandler       the row data handler used to transform messages
     * @param entryHandlerProvider provider of user-defined {@link EntryHandler} beans
     * @return a synchronous message handler implementation
     */
    @Bean
    @ConditionalOnProperty(value = CanalProperties.CANAL_ASYNC, havingValue = "false")
    public MessageHandler syncMessageHandler(CanalProperties properties,
                                         RowDataHandler<CanalEntry.RowData> rowDataHandler,
                                         ObjectProvider<EntryHandler> entryHandlerProvider) {
        return new SyncMessageHandlerImpl(properties.getSubscribeTypes(), entryHandlerProvider.stream().collect(Collectors.toList()), rowDataHandler);
    }

    /**
     * Creates the {@link ClusterCanalClient} and registers it with Spring-managed
     * lifecycle ({@code start} on context refresh, {@code stop} on shutdown).
     * <p>Connectors declared in the application context are merged with the ones
     * declared under {@code canal.cluster.instances}.</p>
     *
     * @param connectorProvider    provider of Spring-managed {@code ClusterCanalConnector} beans
     * @param messageHandlerProvider provider of the {@link MessageHandler} to receive Canal events
     * @param canalProperties      common Canal properties
     * @param connectorProperties  cluster-mode connector configuration providing additional instances
     * @return the started Canal cluster client
     */
    @Bean(initMethod = "start", destroyMethod = "stop")
    public ClusterCanalClient clusterCanalClient(ObjectProvider<ClusterCanalConnector> connectorProvider,
                                                 ObjectProvider<MessageHandler> messageHandlerProvider,
                                                 CanalProperties canalProperties,
                                                 CanalClusterProperties connectorProperties){
        // 1. Collect every Spring-managed ClusterCanalConnector in the context.
        List<ClusterCanalConnector> clusterCanalConnectors = connectorProvider.stream().collect(Collectors.toList());
        // 2. Append connectors declared via configuration properties.
        if(!CollectionUtils.isEmpty(connectorProperties.getInstances())){
            clusterCanalConnectors.addAll(connectorProperties.getInstances().stream()
                    .map(instance -> ConnectorUtil.createClusterCanalConnector(instance))
                    .collect(Collectors.toList()));
        }
        // 3. Build and return the ClusterCanalClient.
        return (ClusterCanalClient) new ClusterCanalClient.Builder()
                .batchSize(canalProperties.getBatchSize())
                .filter(canalProperties.getFilter())
                .timeout(canalProperties.getTimeout())
                .unit(canalProperties.getUnit())
                .messageHandler(messageHandlerProvider.getIfAvailable())
                .build(clusterCanalConnectors);
    }

}
