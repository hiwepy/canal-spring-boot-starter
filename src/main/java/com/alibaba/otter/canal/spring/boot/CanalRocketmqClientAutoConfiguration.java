package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.RocketMQCanalClient;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.factory.MapColumnModelFactory;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.handler.MessageHandler;
import com.alibaba.otter.canal.handler.RowDataHandler;
import com.alibaba.otter.canal.handler.impl.AsyncFlatMessageHandlerImpl;
import com.alibaba.otter.canal.handler.impl.MapRowDataHandlerImpl;
import com.alibaba.otter.canal.handler.impl.SyncFlatMessageHandlerImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
@ConditionalOnClass({ RocketMQCanalConnector.class, DefaultMQPushConsumer.class })
@ConditionalOnProperty(prefix = CanalProperties.CANAL_MODE, havingValue = "rocketmq")
@EnableConfigurationProperties({CanalProperties.class, CanalRocketmqClientProperties.class})
@Slf4j
public class CanalRocketmqClientAutoConfiguration {

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
	@ConditionalOnBean(RocketMQCanalConnector.class)
	public RocketMQCanalConnector defaultRocketMQCanalConnector(CanalRocketmqClientProperties properties) {

		// 1、创建连接实例
		RocketMQCanalConnector connector;
		if (StringUtils.hasText(properties.getAccessKey()) && StringUtils.hasText(properties.getSecretKey())) {
			if (StringUtils.hasText(properties.getNamespace())) {
				connector = new RocketMQCanalConnector(properties.getNameServer(), properties.getTopic(),
						properties.getGroupName(), properties.getAccessKey(), properties.getSecretKey(),
						properties.getBatchSize(), properties.isFlatMessage(), properties.isEnableMessageTrace(), null,
						properties.getAccessChannel(), properties.getNamespace());
			} else if (StringUtils.hasText(properties.getCustomizedTraceTopic())) {
				connector = new RocketMQCanalConnector(properties.getNameServer(), properties.getTopic(),
						properties.getGroupName(), properties.getAccessKey(), properties.getSecretKey(),
						properties.getBatchSize(), properties.isFlatMessage(), properties.isEnableMessageTrace(),
						properties.getCustomizedTraceTopic(), properties.getAccessChannel());
			} else {
				connector = new RocketMQCanalConnector(properties.getNameServer(), properties.getTopic(),
						properties.getGroupName(), properties.getAccessKey(), properties.getSecretKey(),
						properties.getBatchSize(), properties.isFlatMessage());
			}
		} else {
			connector = new RocketMQCanalConnector(properties.getNameServer(), properties.getTopic(),
					properties.getGroupName(), properties.getBatchSize(), properties.isFlatMessage());
		}
		return connector;
	}

	@Bean(initMethod = "start", destroyMethod = "stop")
	public RocketMQCanalClient rocketMQCanalClient(ObjectProvider<RocketMQCanalConnector> connectorProvider,
								 ObjectProvider<MessageHandler> messageHandlerProvider,
								 CanalProperties canalProperties){
		return (RocketMQCanalClient) new RocketMQCanalClient.Builder()
				.batchSize(canalProperties.getBatchSize())
				.filter(canalProperties.getFilter())
				.timeout(canalProperties.getTimeout())
				.unit(canalProperties.getUnit())
				.messageHandler(messageHandlerProvider.getIfAvailable())
				.build(connectorProvider.getIfAvailable());
	}

}
