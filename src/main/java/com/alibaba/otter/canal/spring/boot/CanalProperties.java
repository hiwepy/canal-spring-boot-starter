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

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
	public static final String CANAL_INSTANCES = PREFIX + "." + "instances";

	/**
	 * The mode of the Canal Client.
	 * simple,cluster,kafka,rocketMQ
	 */
	private ClientMode mode = ClientMode.simple;
	/**
	 * 是否异步
	 */
	private boolean async = false;
	/**
	 * The client subscribes to filter, and the corresponding filter information will be updated when the subscription is repeated
	 * <pre>
	 * 说明：
	 * a. 如果本次订阅中filter信息为空，则直接使用canal server服务端配置的filter信息
	 * b. 如果本次订阅中filter信息不为空，目前会直接替换canal server服务端配置的filter信息，以本次提交的为准
	 * </pre>
	 */
	private String filter = StringUtils.EMPTY;
	/**
	 * The number of messages read from the Canal service in each time
	 */
	private Integer batchSize = 1000;
	/**
	 *  -1代表不做timeout控制
	 */
	private Long timeout = -1L;
	/**
	 * 获取数据超时时间单位
	 */
	private TimeUnit unit = TimeUnit.SECONDS;
	/**
	 * 指定订阅的事件类型，主要用于标识事务的开始，变更数据，结束
	 */
	private List<CanalEntry.EntryType> subscribeTypes = Arrays.asList(CanalEntry.EntryType.ROWDATA);

	/**
	 * Canal Server Mode. simple, cluster, kafka, pulsarmq, rabbitmq, rocketmq
	 */
	public enum ClientMode {
		simple, cluster, kafka, pulsarmq, rabbitmq, rocketmq
	}

}
