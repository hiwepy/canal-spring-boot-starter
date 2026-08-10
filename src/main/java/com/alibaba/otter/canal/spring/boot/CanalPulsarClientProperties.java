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

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Connection properties for the Canal <strong>Pulsar</strong> client mode.
 * <p>
 * Bound to the {@code canal.pulsar.*} configuration namespace. Each
 * {@link Instance} describes a Pulsar consumer subscribing to Canal binlog
 * events published to a Pulsar topic.
 * </p>
 *
 * <h3>Configuration keys</h3>
 * <ul>
 *   <li>{@code canal.pulsar.enabled} — whether the Pulsar client is enabled (default {@code false})</li>
 *   <li>{@code canal.pulsar.instances} — list of Pulsar Canal consumer definitions</li>
 * </ul>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@ConfigurationProperties(CanalPulsarClientProperties.PREFIX)
@Data
public class CanalPulsarClientProperties {

    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_MAX_SLEEP_MS = Integer.MAX_VALUE;
	/** Configuration prefix used by Spring Boot to bind properties. */
	public static final String PREFIX = "canal.pulsar";

	/**
	 * Whether to enable the Canal Pulsar client. When {@code false} (the default)
	 * the Pulsar auto-configuration is skipped.
	 */
	private boolean enabled = false;

	/**
	 * List of Pulsar Canal consumer connection definitions. Each entry produces
	 * one {@code PulsarMQCanalConnector} when the Pulsar client is built.
	 */
	private List<CanalPulsarClientProperties.Instance> instances = new ArrayList<>();

	/**
	 * Connection definition for a single Pulsar Canal consumer.
	 */
	@Data
	public static class Instance {

		/** Pulsar broker service URL. */
		private String serviceUrl;
		/** Authentication role token used by the Pulsar client. */
		private String roleToken;
		/** Pulsar topic that Canal publishes binlog events to. */
		private String topic;
		/** Pulsar subscription name used by the consumer. */
		private String subscriptName;
		/** Maximum number of messages fetched per batch. Defaults to {@code 30}. */
		private int batchSize = 30;
		/**
		 * Combined with {@link #batchSize} to control the batch fetch behaviour. A
		 * batch is delivered when either {@code batchSize} messages have
		 * accumulated or {@code batchTimeoutSeconds} elapse since the last batch.
		 */
		private int batchTimeoutSeconds = 30;
		/**
		 * Timeout in seconds for processing a single batch. Should be tuned in
		 * line with {@link #batchSize} and {@link #batchTimeoutSeconds}.
		 */
		private int batchProcessTimeoutSeconds = 60;
		/** Delay in seconds before redelivering failed messages. Defaults to {@code 60}. */
		private int redeliveryDelaySeconds = 60;
		/** Ack timeout in seconds; unacked messages are redelivered after this period. */
		private int ackTimeoutSeconds = 30;
		/** Whether to enable the message retry feature. Defaults to {@code true}. */
		private boolean retry = true;
		/** Whether the retry ({@code -RETRY}) and dead-letter ({@code -DLQ}) suffixes are upper-case. */
		private boolean retryDLQUpperCase = false;
		/** Maximum number of redelivery attempts before a message is moved to the dead-letter queue. */
		private int maxRedeliveryCount = 128;
		/** Whether Canal messages are flattened (plain JSON) on the broker side. */
		private boolean flatMessage = false;

	}


}
