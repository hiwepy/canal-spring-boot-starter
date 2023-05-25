package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import com.alibaba.otter.canal.client.kafka.KafkaOffsetCanalConnector;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass({ KafkaCanalConnector.class, KafkaConsumer.class })
@ConditionalOnProperty(prefix = CanalKafkaProperties.PREFIX, value = "enabled", havingValue = "true")
@EnableConfigurationProperties(CanalKafkaProperties.class)
@Slf4j
public class CanalKafkaAutoConfiguration {

    @Bean(initMethod = "connect", destroyMethod = "disconnect")
    public KafkaCanalConnector kafkaCanalConnector(CanalKafkaProperties properties) {


        final KafkaCanalConnector connector = properties.isEarliest() ? new KafkaOffsetCanalConnector(properties.getServers(),
                properties.getTopic(),  properties.getPartition(), properties.getGroupId(),
                properties.isFlatMessage()) : new KafkaCanalConnector(properties.getServers(),
                properties.getTopic(),  properties.getPartition(), properties.getGroupId(),
                properties.getBatchSize(), properties.isFlatMessage());
        try {


        } catch (Throwable e) {
            log.error("## Something going wrong when starting up the rocketmq consumer:", e);
            System.exit(0);
        }

        return connector;
    }

}
