package com.alibaba.otter.canal.spring.boot;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;

import lombok.extern.slf4j.Slf4j;

@Configuration
@ConditionalOnClass({ RocketMQCanalConnector.class, DefaultMQPushConsumer.class })
@ConditionalOnProperty(prefix = CanalRocketMQProperties.PREFIX, value = "enabled", havingValue = "true")
@EnableConfigurationProperties(CanalRocketMQProperties.class)
@Slf4j
public class CanalRocketMQAutoConfiguration {

	@Bean(destroyMethod = "disconnect")
	public RocketMQCanalConnector rocketMQCanalConnector(CanalRocketMQProperties properties) {

		// 1、创建连接实例
		RocketMQCanalConnector connector = null;
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
		
		log.info("## Start the rocketmq consumer: {}-{}", properties.getTopic(), properties.getGroupName());
		connector.subscribe();
		
		log.info("## The canal rocketmq consumer is running now ......");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
            	log.info("## Stop the rocketmq consumer");
               // rocketMQClientExample.stop();
            } catch (Throwable e) {
            	log.warn("## Something goes wrong when stopping rocketmq consumer:", e);
            } finally {
            	log.info("## Rocketmq consumer is down.");
            }
        }));
		
		
		/**
		 * 应用退出时，要调用shutdown来清理资源，关闭网络连接，从RocketMQ服务器上注销自己
		 * 注意：我们建议应用在JBOSS、Tomcat等容器的退出钩子里调用shutdown方法
		 */
		Runtime.getRuntime().addShutdownHook(new CanalShutdownHook(connector));
		return connector;
	}

}
