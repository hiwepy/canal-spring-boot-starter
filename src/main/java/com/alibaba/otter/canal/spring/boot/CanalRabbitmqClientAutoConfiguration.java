package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.RabbitMQCanalClient;
import com.alibaba.otter.canal.client.rabbitmq.RabbitMQCanalConnector;
import com.alibaba.otter.canal.factory.MapColumnModelFactory;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.handler.MessageHandler;
import com.alibaba.otter.canal.handler.RowDataHandler;
import com.alibaba.otter.canal.handler.impl.AsyncFlatMessageHandlerImpl;
import com.alibaba.otter.canal.handler.impl.MapRowDataHandlerImpl;
import com.alibaba.otter.canal.handler.impl.SyncFlatMessageHandlerImpl;
import com.alibaba.otter.canal.util.ConnectorUtil;
import com.rabbitmq.client.DefaultConsumer;
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
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
@ConditionalOnClass({ RabbitMQCanalConnector.class, DefaultConsumer.class })
@ConditionalOnProperty(value = CanalProperties.CANAL_MODE, havingValue = "rabbitmq")
@EnableConfigurationProperties({CanalProperties.class, CanalRabbitmqClientProperties.class})
@Import(CanalThreadPoolAutoConfiguration.class)
@Slf4j
public class CanalRabbitmqClientAutoConfiguration {

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
	public List<RabbitMQCanalConnector> rabbitMQCanalConnectors(CanalRabbitmqClientProperties connectorProperties){
		Assert.notEmpty(connectorProperties.getInstances(), "No rabbitMQ canal instance configured");
		return connectorProperties.getInstances().stream()
				.map(instance -> ConnectorUtil.createRabbitMQCanalConnector(instance))
				.collect(Collectors.toList());
	}

	@Bean(initMethod = "start", destroyMethod = "stop")
	public RabbitMQCanalClient rabbitMQCanalClient(ObjectProvider<RabbitMQCanalConnector> connectorProvider,
												ObjectProvider<MessageHandler> messageHandlerProvider,
												CanalProperties canalProperties){
		return (RabbitMQCanalClient) new RabbitMQCanalClient.Builder()
				.batchSize(canalProperties.getBatchSize())
				.filter(canalProperties.getFilter())
				.timeout(canalProperties.getTimeout())
				.unit(canalProperties.getUnit())
				.messageHandler(messageHandlerProvider.getIfAvailable())
				.build(connectorProvider.stream().collect(Collectors.toList()));
	}

}
