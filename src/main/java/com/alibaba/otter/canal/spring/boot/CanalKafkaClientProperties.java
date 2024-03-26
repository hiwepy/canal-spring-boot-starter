/*
 * Copyright (c) 2018, hiwepy (https://github.com/hiwepy).
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.alibaba.otter.canal.spring.boot;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author ： <a href="https://github.com/hiwepy">hiwepy</a>
 */
@ConfigurationProperties(CanalKafkaClientProperties.PREFIX)
@Data
public class CanalKafkaClientProperties {

    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_MAX_SLEEP_MS = Integer.MAX_VALUE;

	public static final String PREFIX = "canal.kafka";

	/**
	 * Whether Enable Canal KafkaMQ.
	 */
	private boolean enabled = false;

	/**
	 * 配置信息
	 */
	private List<CanalKafkaClientProperties.Instance> instances = new ArrayList<>();

	@Data
	public static class Instance {

		/**
		 * 启动时从未消费的消息位置开始
		 */
		boolean earliest = true;
		/**
		 * 消息分区索引
		 */
		Integer partition;
		/**
		 * Kafka服务器地址
		 */
		String servers;
		/**
		 * 订阅的消息主题
		 */
		String topic;
		/**
		 * 消费者组ID
		 */
		String groupId;
		/**
		 * 批量获取数据的大小
		 */
		Integer batchSize;
		/**
		 * 是否扁平化Canal消息内容
		 */
		boolean flatMessage;

	}


}
