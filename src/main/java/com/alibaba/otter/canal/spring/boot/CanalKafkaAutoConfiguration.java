package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import com.alibaba.otter.canal.client.kafka.KafkaOffsetCanalConnector;
import com.alibaba.otter.canal.spring.boot.hooks.CanalShutdownHook;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass({ KafkaCanalConnector.class, KafkaConsumer.class })
@ConditionalOnProperty(prefix = CanalProperties.PREFIX, value = "server-mode", havingValue = "KAFKA")
@EnableConfigurationProperties(CanalKafkaProperties.class)
@Slf4j
public class CanalKafkaAutoConfiguration {

    @Bean(initMethod = "connect", destroyMethod = "disconnect")
    @ConditionalOnBean(KafkaCanalConnector.class)
    public KafkaCanalConnector defaultKafkaCanalConnector(CanalKafkaProperties properties) {
        KafkaCanalConnector connector = properties.isEarliest() ? new KafkaOffsetCanalConnector(properties.getServers(),
                properties.getTopic(),  properties.getPartition(), properties.getGroupId(),
                properties.isFlatMessage()) : new KafkaCanalConnector(properties.getServers(),
                properties.getTopic(),  properties.getPartition(), properties.getGroupId(),
                properties.getBatchSize(), properties.isFlatMessage());
        Runtime.getRuntime().addShutdownHook(new CanalShutdownHook(connector));
        return connector;
    }

}
