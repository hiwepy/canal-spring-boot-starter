/*
 * Copyright (c) 2018, hiwepy (https://github.com/easy-4-java).
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
import org.apache.commons.lang.StringUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Root configuration properties for the Alibaba Canal Spring Boot starter.
 * <p>
 * Bound to the {@code canal.*} configuration namespace. Defines the common
 * options shared by every Canal client mode (simple, cluster, kafka, pulsarmq,
 * rabbitmq, rocketmq). Mode-specific connection settings live under their own
 * nested namespaces (e.g. {@code canal.simple.*}, {@code canal.kafka.*}).
 * </p>
 *
 * <h3>Configuration keys</h3>
 * <ul>
 *   <li>{@code canal.mode} — Canal client mode (default {@code simple})</li>
 *   <li>{@code canal.async} — whether message handling runs asynchronously (default {@code true})</li>
 *   <li>{@code canal.filter} — Canal subscription filter expression (default empty)</li>
 *   <li>{@code canal.batch-size} — number of messages fetched per batch (default {@code 1000})</li>
 *   <li>{@code canal.timeout} — polling timeout; {@code -1} disables timeout control (default {@code -1})</li>
 *   <li>{@code canal.unit} — timeout time unit (default {@code SECONDS})</li>
 *   <li>{@code canal.subscribe-types} — entry types to subscribe to (default {@code ROWDATA})</li>
 * </ul>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@ConfigurationProperties(CanalProperties.PREFIX)
public class CanalProperties {

	/** Configuration prefix used by Spring Boot to bind properties. */
	public static final String PREFIX = "canal";

	/** Full property key for {@code canal.async}. */
	public static final String CANAL_ASYNC = PREFIX + "." + "async";
	/** Full property key for {@code canal.mode}. */
	public static final String CANAL_MODE = PREFIX + "." + "mode";
	/** Full property key for {@code canal.instances}. */
	public static final String CANAL_INSTANCES = PREFIX + "." + "instances";

	/**
	 * The mode of the Canal client.
	 * <p>One of {@code simple}, {@code cluster}, {@code kafka}, {@code pulsarmq},
	 * {@code rabbitmq} or {@code rocketmq}. Determines which auto-configuration
	 * and connector implementation is activated.</p>
	 */
	private ClientMode mode = ClientMode.simple;
	/**
	 * Whether to dispatch messages to handlers asynchronously using the Canal
	 * thread pool. When {@code null} the default behaviour of each
	 * auto-configuration applies (asynchronous by default).
	 */
	private Boolean async;
	/**
	 * Canal subscription filter expression. The corresponding filter information
	 * is updated when the subscription is repeated.
	 * <pre>
	 * Notes:
	 * a. If the filter is empty on subscription, the Canal server-side configured filter is used.
	 * b. If the filter is not empty, it replaces the Canal server-side filter.
	 * </pre>
	 */
	private String filter = StringUtils.EMPTY;
	/** Number of messages read from the Canal service on each poll. */
	private Integer batchSize = 1000;
	/** Polling timeout; {@code -1} disables timeout control. */
	private Long timeout = -1L;
	/** Time unit applied to {@link #timeout}. */
	private TimeUnit unit = TimeUnit.SECONDS;
	/**
	 * Entry types to subscribe to, mainly used to mark transaction begin, changed
	 * data and transaction end.
	 */
	private List<CanalEntry.EntryType> subscribeTypes = Arrays.asList(CanalEntry.EntryType.ROWDATA);

	public ClientMode getMode() { return mode; }
	public void setMode(ClientMode mode) { this.mode = mode; }
	public Boolean getAsync() { return async; }
	public void setAsync(Boolean async) { this.async = async; }
	public String getFilter() { return filter; }
	public void setFilter(String filter) { this.filter = filter; }
	public Integer getBatchSize() { return batchSize; }
	public void setBatchSize(Integer batchSize) { this.batchSize = batchSize; }
	public Long getTimeout() { return timeout; }
	public void setTimeout(Long timeout) { this.timeout = timeout; }
	public TimeUnit getUnit() { return unit; }
	public void setUnit(TimeUnit unit) { this.unit = unit; }
	public List<CanalEntry.EntryType> getSubscribeTypes() { return subscribeTypes; }
	public void setSubscribeTypes(List<CanalEntry.EntryType> subscribeTypes) { this.subscribeTypes = subscribeTypes; }

	/**
	 * Canal client connection mode.
	 * <p>Supported values: {@code simple}, {@code cluster}, {@code kafka},
	 * {@code pulsarmq}, {@code rabbitmq}, {@code rocketmq}.</p>
	 */
	public enum ClientMode {
		/** Direct single-node TCP connection to a Canal server. */
		simple,
		/** High-availability cluster connection backed by ZooKeeper. */
		cluster,
		/** Consume Canal data published to Kafka. */
		kafka,
		/** Consume Canal data published to Pulsar. */
		pulsarmq,
		/** Consume Canal data published to RabbitMQ. */
		rabbitmq,
		/** Consume Canal data published to RocketMQ. */
		rocketmq
	}

}
