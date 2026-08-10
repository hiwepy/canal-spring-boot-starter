package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.SimpleCanalClient;
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
import org.springframework.beans.factory.annotation.Qualifier;
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

/**
 * Spring Boot auto-configuration for the Canal <strong>simple</strong> client.
 * <p>
 * Activated by default (when {@code canal.mode=simple} or the property is
 * absent) and wires a direct single-node TCP {@link SimpleCanalConnector} to a
 * {@link SimpleCanalClient}. It registers a {@link RowDataHandler}, an async or
 * sync {@link MessageHandler} depending on {@code canal.async}, and the client
 * bean whose lifecycle (start/stop) is managed by Spring.
 * </p>
 *
 * <h3>Configuration keys</h3>
 * <ul>
 *   <li>{@code canal.mode} — must be {@code simple} (default)</li>
 *   <li>{@code canal.async} — whether message dispatch is asynchronous (default {@code true})</li>
 *   <li>{@code canal.simple.instances} — list of Canal server instances to connect to</li>
 * </ul>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@Configuration
@ConditionalOnClass({ SimpleCanalConnector.class })
@ConditionalOnProperty(value = CanalProperties.CANAL_MODE, havingValue = "simple", matchIfMissing = true)
@EnableConfigurationProperties({CanalProperties.class, CanalSimpleProperties.class})
@Import(CanalThreadPoolAutoConfiguration.class)
@Slf4j
public class CanalSimpleClientAutoConfiguration {

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
                                            @Qualifier("canalTaskExecutor") ThreadPoolTaskExecutor threadPoolTaskExecutor) {
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
     * Creates the {@link SimpleCanalClient} and registers it with Spring-managed
     * lifecycle ({@code start} on context refresh, {@code stop} on shutdown).
     * <p>Connectors declared in the application context are merged with the ones
     * declared under {@code canal.simple.instances}.</p>
     *
     * @param connectorProvider    provider of Spring-managed {@link SimpleCanalConnector} beans
     * @param messageHandler       the message handler to receive Canal events
     * @param canalProperties      common Canal properties
     * @param connectorProperties  simple-mode connector configuration providing additional instances
     * @return the started Canal simple client
     */
    @Bean(initMethod = "start", destroyMethod = "stop")
    public SimpleCanalClient simpleCanalClient(ObjectProvider<SimpleCanalConnector> connectorProvider,
                                               MessageHandler messageHandler,
                                               CanalProperties canalProperties,
                                               CanalSimpleProperties connectorProperties){
        // 1. Collect every Spring-managed SimpleCanalConnector in the context.
        List<SimpleCanalConnector> simpleCanalConnectors = connectorProvider.stream().collect(Collectors.toList());
        // 2. Append connectors declared via configuration properties.
        if(!CollectionUtils.isEmpty(connectorProperties.getInstances())){
            simpleCanalConnectors.addAll(connectorProperties.getInstances().stream()
                    .map(instance -> ConnectorUtil.createSimpleCanalConnector(instance))
                    .collect(Collectors.toList()));
        }
        // 3. Build and return the SimpleCanalClient.
        return (SimpleCanalClient) new SimpleCanalClient.Builder()
                .batchSize(canalProperties.getBatchSize())
                .filter(canalProperties.getFilter())
                .timeout(canalProperties.getTimeout())
                .unit(canalProperties.getUnit())
                .messageHandler(messageHandler)
                .setSubscribeTypes(canalProperties.getSubscribeTypes())
                .build(simpleCanalConnectors);
    }

}
