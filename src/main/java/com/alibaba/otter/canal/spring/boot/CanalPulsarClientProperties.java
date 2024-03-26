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
 * PulsarMQ
 * @author ： <a href="https://github.com/hiwepy">hiwepy</a>
 */
@ConfigurationProperties(CanalPulsarClientProperties.PREFIX)
@Data
public class CanalPulsarClientProperties {

    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_MAX_SLEEP_MS = Integer.MAX_VALUE;
	public static final String PREFIX = "canal.pulsar";

	/**
	 * Whether Enable Canal PulsarMQ.
	 */
	private boolean enabled = false;

	/**
	 * 配置信息
	 */
	private List<CanalPulsarClientProperties.Instance> instances = new ArrayList<>();

	@Data
	public static class Instance {

		/**
		 * PulsarMQ 服务地址
		 */
		private String serviceUrl;
		/**
		 * 角色认证 token
		 */
		private String roleToken;
		/**
		 * 订阅的主题
		 */
		private String topic;
		/**
		 * 订阅客户端名称
		 */
		private String subscriptName;
		/**
		 * 每次批量获取数据的最大条目数，默认30
		 */
		private int batchSize = 30;
		/**
		 * 与{@code batchSize}一起决定批量获取的数据大小
		 * 当：
		 * <p>
		 * 1. {@code batchSize} 条消息未消费时<br/>
		 * 2. 距上一次批量消费时间达到{@code batchTimeoutSeconds}秒时
		 * </p>
		 * 任一条件满足，即执行批量消费
		 */
		private int batchTimeoutSeconds = 30;
		/**
		 * 批量处理消息时，一次批量处理的超时时间秒数
		 * <p>
		 * 该时间应该根据{@code batchSize}和{@code batchTimeoutSeconds}合理设置
		 * </p>
		 */
		private int batchProcessTimeoutSeconds = 60;
		/**
		 * 消费失败后的重试秒数，默认60秒
		 */
		private int redeliveryDelaySeconds = 60;
		/**
		 * 当客户端接收到消息，30秒还没有返回ack给服务端时，ack超时，会重新消费该消息
		 */
		private int ackTimeoutSeconds = 30;
		/**
		 * 是否开启消息失败重试功能，默认开启
		 */
		private boolean retry = true;
		/**
		 * <p>
		 * true重试(-RETRY)和死信队列(-DLQ)后缀为大写，有些地方创建的为小写，需确保正确
		 * </p>
		 */
		private boolean retryDLQUpperCase = false;
		/**
		 * 最大重试次数
		 */
		private int maxRedeliveryCount = 128;
		/**
		 * 是否扁平化Canal消息内容
		 */
		private boolean flatMessage = false;

	}


}
