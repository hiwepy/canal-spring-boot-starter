package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.rabbitmq.RabbitMQCanalConnector;
import com.rabbitmq.client.DefaultConsumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass({ RabbitMQCanalConnector.class, DefaultConsumer.class })
@ConditionalOnProperty(prefix = CanalRabbitMQProperties.PREFIX, value = "enabled", havingValue = "true")
@EnableConfigurationProperties(CanalRabbitMQProperties.class)
@Slf4j
public class CanalRabbitMQAutoConfiguration {

	@Bean(initMethod = "connect", destroyMethod = "disconnect")
	public RabbitMQCanalConnector rabbitMQCanalConnector(CanalRabbitMQProperties properties) {
		final RabbitMQCanalConnector connector = new RabbitMQCanalConnector(properties.getNameServer(), properties.getVhost(),
				properties.getQueueName(), properties.getAccessKey(), properties.getSecretKey(),
				properties.getUsername(), properties.getPassword(), properties.getResourceOwnerId(),
				properties.isFlatMessage());
		try {
			
			 
        } catch (Throwable e) {
            log.error("## Something going wrong when starting up the rocketmq consumer:", e);
            System.exit(0);
        }
		
		return connector;
	}

}
