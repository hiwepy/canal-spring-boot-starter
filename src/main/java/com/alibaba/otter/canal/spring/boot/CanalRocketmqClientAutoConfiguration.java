package com.alibaba.otter.canal.spring.boot;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.client.RocketMQCanalClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.factory.MapColumnModelFactory;
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
import com.alibaba.otter.canal.handler.impl.AsyncFlatMessageHandlerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.handler.impl.MapRowDataHandlerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.handler.impl.SyncFlatMessageHandlerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.util.ConnectorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
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
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.stream.Collectors;

/**
 * Spring Boot auto-configuration for the Canal <strong>RocketMQ</strong> client.
 * <p>
 * Activated when {@code canal.mode=rocketmq} and consumes Canal binlog events
 * published to RocketMQ topics via {@code RocketMQCanalConnector} instances
 * wired into a {@link RocketMQCanalClient}. Registers a {@link RowDataHandler},
 * an async or sync flat-message {@link MessageHandler} based on
 * {@code canal.async}, and the Spring-managed client bean.
 * </p>
 *
 * <h3>Configuration keys</h3>
 * <ul>
 *   <li>{@code canal.mode} — must be {@code rocketmq}</li>
 *   <li>{@code canal.async} — whether message dispatch is asynchronous (default {@code true})</li>
 *   <li>{@code canal.rocketmq.instances} — list of RocketMQ Canal consumer definitions</li>
 * </ul>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@Configuration
@ConditionalOnClass({ RocketMQCanalConnector.class, DefaultMQPushConsumer.class })
@ConditionalOnProperty(value = CanalProperties.CANAL_MODE, havingValue = "rocketmq")
@EnableConfigurationProperties({CanalProperties.class, CanalRocketmqClientProperties.class})
@Import(CanalThreadPoolAutoConfiguration.class)
public class CanalRocketmqClientAutoConfiguration {

	/**
	 * Creates the {@link RowDataHandler} used to convert flat Canal messages
	 * into lists of column maps.
	 *
	 * @return a row data handler backed by the {@link MapColumnModelFactory}
	 */
	@Bean
	public RowDataHandler<List<Map<String, String>>> rowDataHandler() {
		return new MapRowDataHandlerImpl(new MapColumnModelFactory());
	}

	/**
	 * Creates the asynchronous flat-message {@link MessageHandler} used when {@code canal.async=true}.
	 *
	 * @param properties           common Canal properties providing the entry types to subscribe to
	 * @param rowDataHandler       the row data handler used to transform messages
	 * @param entryHandlerProvider provider of user-defined {@link EntryHandler} beans
	 * @param canalTaskExecutor    the Canal task executor used for async dispatch
	 * @return an asynchronous flat-message handler implementation
	 */
	@Bean
	@ConditionalOnProperty(value = CanalProperties.CANAL_ASYNC, havingValue = "true", matchIfMissing = true)
	public MessageHandler asyncMessageHandler(CanalProperties properties,
										 RowDataHandler<List<Map<String, String>>> rowDataHandler,
										 ObjectProvider<EntryHandler> entryHandlerProvider,
										 @Qualifier("canalTaskExecutor") ThreadPoolTaskExecutor canalTaskExecutor) {
		return new AsyncFlatMessageHandlerImpl(properties.getSubscribeTypes(), entryHandlerProvider.stream().collect(Collectors.toList()), rowDataHandler, canalTaskExecutor);
	}

	/**
	 * Creates the synchronous flat-message {@link MessageHandler} used when {@code canal.async=false}.
	 *
	 * @param properties           common Canal properties providing the entry types to subscribe to
	 * @param rowDataHandler       the row data handler used to transform messages
	 * @param entryHandlerProvider provider of user-defined {@link EntryHandler} beans
	 * @return a synchronous flat-message handler implementation
	 */
	@Bean
	@ConditionalOnProperty(value = CanalProperties.CANAL_ASYNC, havingValue = "false")
	public MessageHandler syncMessageHandler(CanalProperties properties,
										 RowDataHandler<List<Map<String, String>>> rowDataHandler,
										 ObjectProvider<EntryHandler> entryHandlerProvider) {
		return new SyncFlatMessageHandlerImpl(properties.getSubscribeTypes(), entryHandlerProvider.stream().collect(Collectors.toList()), rowDataHandler);
	}

	/**
	 * Creates the {@link RocketMQCanalClient} and registers it with Spring-managed
	 * lifecycle ({@code start} on context refresh, {@code stop} on shutdown).
	 * <p>Connectors declared in the application context are merged with the ones
	 * declared under {@code canal.rocketmq.instances}.</p>
	 *
	 * @param connectorProvider    provider of Spring-managed {@code RocketMQCanalConnector} beans
	 * @param messageHandlerProvider provider of the {@link MessageHandler} to receive Canal events
	 * @param canalProperties      common Canal properties
	 * @param connectorProperties  RocketMQ-mode connector configuration providing additional instances
	 * @return the started Canal RocketMQ client
	 */
	@Bean(initMethod = "start", destroyMethod = "stop")
	public RocketMQCanalClient rocketMQCanalClient(ObjectProvider<RocketMQCanalConnector> connectorProvider,
								 	ObjectProvider<MessageHandler> messageHandlerProvider,
								 	CanalProperties canalProperties,
							   		CanalRocketmqClientProperties connectorProperties){
		// 1. Collect every Spring-managed RocketMQCanalConnector in the context.
		List<RocketMQCanalConnector> rocketMQCanalConnectors = connectorProvider.stream().collect(Collectors.toList());
		// 2. Append connectors declared via configuration properties.
		if(!CollectionUtils.isEmpty(connectorProperties.getInstances())){
			rocketMQCanalConnectors.addAll(connectorProperties.getInstances().stream()
					.map(instance -> ConnectorUtil.createRocketMQCanalConnector(instance))
					.collect(Collectors.toList()));
		}
		// 3. Build and return the RocketMQCanalClient.
		return (RocketMQCanalClient) new RocketMQCanalClient.Builder()
				.batchSize(canalProperties.getBatchSize())
				.filter(canalProperties.getFilter())
				.timeout(canalProperties.getTimeout())
				.unit(canalProperties.getUnit())
				.messageHandler(messageHandlerProvider.getIfAvailable())
				.build(rocketMQCanalConnectors);
	}

}
