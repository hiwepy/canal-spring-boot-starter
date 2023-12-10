package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.rabbitmq.RabbitMQCanalConnector;
import com.alibaba.otter.canal.spring.boot.hooks.CanalShutdownHook;
import com.rabbitmq.client.DefaultConsumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass({ RabbitMQCanalConnector.class, DefaultConsumer.class })
@ConditionalOnProperty(prefix = CanalProperties.PREFIX, value = "server-mode", havingValue = "RABBIT_MQ")
@EnableConfigurationProperties(CanalRabbitMQProperties.class)
@Slf4j
public class CanalRabbitMQAutoConfiguration {

	@Bean(initMethod = "connect", destroyMethod = "disconnect")
	@ConditionalOnBean(RabbitMQCanalConnector.class)
	public RabbitMQCanalConnector defaultRabbitMQCanalConnector(CanalRabbitMQProperties properties) {
		RabbitMQCanalConnector connector = new RabbitMQCanalConnector(properties.getAddresses(), properties.getVhost(),
				properties.getQueueName(), properties.getAccessKey(), properties.getSecretKey(),
				properties.getUsername(), properties.getPassword(), properties.getResourceOwnerId(),
				properties.isFlatMessage());
		Runtime.getRuntime().addShutdownHook(new CanalShutdownHook(connector));
		return connector;
	}

}
