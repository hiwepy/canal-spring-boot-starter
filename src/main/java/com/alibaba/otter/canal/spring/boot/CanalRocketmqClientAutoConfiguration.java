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
import com.alibaba.otter.canal.util.ConnectorUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
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
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
@ConditionalOnClass({ RocketMQCanalConnector.class, DefaultMQPushConsumer.class })
@ConditionalOnProperty(value = CanalProperties.CANAL_MODE, havingValue = "rocketmq")
@EnableConfigurationProperties({CanalProperties.class, CanalRocketmqClientProperties.class})
@Import(CanalThreadPoolAutoConfiguration.class)
@Slf4j
public class CanalRocketmqClientAutoConfiguration {

	@Bean
	public RowDataHandler<List<Map<String, String>>> rowDataHandler() {
		return new MapRowDataHandlerImpl(new MapColumnModelFactory());
	}

	@Bean
	@ConditionalOnProperty(value = CanalProperties.CANAL_ASYNC, havingValue = "true")
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

	@Bean(initMethod = "start", destroyMethod = "stop")
	public RocketMQCanalClient rocketMQCanalClient(ObjectProvider<RocketMQCanalConnector> connectorProvider,
								 	ObjectProvider<MessageHandler> messageHandlerProvider,
								 	CanalProperties canalProperties,
							   		CanalRocketmqClientProperties connectorProperties){
		// 1. 获取Spring 上下文中所有的 RocketMQCanalConnector
		List<RocketMQCanalConnector> rocketMQCanalConnectors = connectorProvider.stream().collect(Collectors.toList());
		// 2. 初始化配置文件中配置的 SimpleCanalConnector
		if(!CollectionUtils.isEmpty(connectorProperties.getInstances())){
			rocketMQCanalConnectors.addAll(connectorProperties.getInstances().stream()
					.map(instance -> ConnectorUtil.createRocketMQCanalConnector(instance))
					.collect(Collectors.toList()));
		}
		// 3. 返回 RocketMQCanalClient
		return (RocketMQCanalClient) new RocketMQCanalClient.Builder()
				.batchSize(canalProperties.getBatchSize())
				.filter(canalProperties.getFilter())
				.timeout(canalProperties.getTimeout())
				.unit(canalProperties.getUnit())
				.messageHandler(messageHandlerProvider.getIfAvailable())
				.build(rocketMQCanalConnectors);
	}

}
