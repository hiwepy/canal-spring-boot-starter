package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.spring.boot.event.MessageEvent;
import com.alibaba.otter.canal.spring.boot.hooks.CanalShutdownHook;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

@Configuration
@ConditionalOnClass({ RocketMQCanalConnector.class, DefaultMQPushConsumer.class })
@ConditionalOnProperty(prefix = CanalRocketMQProperties.PREFIX, value = "enabled", havingValue = "true")
@EnableConfigurationProperties(CanalRocketMQProperties.class)
@Slf4j
public class CanalRocketMQAutoConfiguration {

	@Bean(initMethod = "connect", destroyMethod = "disconnect")
	public RocketMQCanalConnector rocketMQCanalConnector(CanalRocketMQProperties properties,
														 @Qualifier("canalDisruptor") Disruptor<MessageEvent> canalDisruptor) {

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
		


		Runtime.getRuntime().addShutdownHook(new CanalShutdownHook(connector));
		return connector;
	}

}
