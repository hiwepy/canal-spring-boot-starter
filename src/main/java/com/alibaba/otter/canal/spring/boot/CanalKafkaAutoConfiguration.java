package com.alibaba.otter.canal.spring.boot;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(CanalKafkaProperties.class)
public class CanalKafkaAutoConfiguration {
 
	
}
