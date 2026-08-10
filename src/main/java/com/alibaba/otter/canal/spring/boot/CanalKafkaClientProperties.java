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

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Connection properties for the Canal <strong>Kafka</strong> client mode.
 * <p>
 * Bound to the {@code canal.kafka.*} configuration namespace. Each
 * {@link Instance} describes a Kafka consumer subscribing to Canal binlog
 * events published to a Kafka topic.
 * </p>
 *
 * <h3>Configuration keys</h3>
 * <ul>
 *   <li>{@code canal.kafka.enabled} — whether the Kafka client is enabled (default {@code false})</li>
 *   <li>{@code canal.kafka.instances} — list of Kafka Canal consumer definitions</li>
 * </ul>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@ConfigurationProperties(CanalKafkaClientProperties.PREFIX)
public class CanalKafkaClientProperties {

    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_MAX_SLEEP_MS = Integer.MAX_VALUE;

	/** Configuration prefix used by Spring Boot to bind properties. */
	public static final String PREFIX = "canal.kafka";

	/**
	 * Whether to enable the Canal Kafka client. When {@code false} (the default)
	 * the Kafka auto-configuration is skipped.
	 */
	private boolean enabled = false;

	/**
	 * List of Kafka Canal consumer connection definitions. Each entry produces
	 * one {@code KafkaCanalConnector} when the Kafka client is built.
	 */
	private List<CanalKafkaClientProperties.Instance> instances = new ArrayList<>();

	/**
	 * Connection definition for a single Kafka Canal consumer.
	 */
	public static class Instance {

		/** Whether to start consuming from the earliest available offset. */
		private boolean earliest = true;
		/** Kafka partition index to consume from, or {@code null} to consume all partitions. */
		private Integer partition;
		/** Comma-separated Kafka broker server addresses. */
		private String servers;
		/** Kafka topic that Canal publishes binlog events to. */
		private String topic;
		/** Kafka consumer group id. */
		private String groupId;
		/** Number of messages fetched per batch, or {@code null} to use the client default. */
		private Integer batchSize;
		/** Whether Canal messages are flattened (plain JSON) on the broker side. */
		private boolean flatMessage;

		public boolean isEarliest() { return earliest; }
		public void setEarliest(boolean earliest) { this.earliest = earliest; }
		public Integer getPartition() { return partition; }
		public void setPartition(Integer partition) { this.partition = partition; }
		public String getServers() { return servers; }
		public void setServers(String servers) { this.servers = servers; }
		public String getTopic() { return topic; }
		public void setTopic(String topic) { this.topic = topic; }
		public String getGroupId() { return groupId; }
		public void setGroupId(String groupId) { this.groupId = groupId; }
		public Integer getBatchSize() { return batchSize; }
		public void setBatchSize(Integer batchSize) { this.batchSize = batchSize; }
		public boolean isFlatMessage() { return flatMessage; }
		public void setFlatMessage(boolean flatMessage) { this.flatMessage = flatMessage; }

	}

	public boolean isEnabled() { return enabled; }
	public void setEnabled(boolean enabled) { this.enabled = enabled; }
	public List<CanalKafkaClientProperties.Instance> getInstances() { return instances; }
	public void setInstances(List<CanalKafkaClientProperties.Instance> instances) { this.instances = instances; }

}
