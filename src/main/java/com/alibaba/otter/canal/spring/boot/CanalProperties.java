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

/**
 *
 * @author ： <a href="https://github.com/hiwepy">hiwepy</a>
 */
@ConfigurationProperties(CanalProperties.PREFIX)
@Data
public class CanalProperties {

	public static final String PREFIX = "canal";

	public static final String CANAL_ASYNC = PREFIX + "." + "async";
	public static final String CANAL_MODE = PREFIX + "." + "mode";

	/**
	 * The consumerMode of the Canal Client.
	 */
	private ConsumerMode consumerMode = ConsumerMode.THREAD_SINGLE;
	/**
	 * The serverMode of the Canal Client.
	 */
	private ServerMode serverMode = ServerMode.TCP;

	/**
	 * Canal Consumer Mode. threadPool, disruptor
	 */
	public enum ConsumerMode {
		THREAD_SINGLE, THREAD_POOL, DISRUPTOR
	}

	/**
	 * Canal Server Mode. TCP, KAFKA, ROCKETMQ, RABBITMQ, PULSARMQ
	 */
	public enum ServerMode {
		TCP, KAFKA, PULSAR_MQ, RABBIT_MQ, ROCKET_MQ
	}

}
