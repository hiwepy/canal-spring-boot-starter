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
/**
 * <p>Auto-configuration for CanalPulsarClientProperties.</p>
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
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
	public static class Instance {

		/** Pulsar broker service URL. */
		private String serviceUrl;
		/** Authentication role token used by the Pulsar client. */
		private String roleToken;
		/** Pulsar topic that Canal publishes binlog events to. */
		private String topic;
		/** Pulsar subscription name used by the consumer. */
		private String subscriptName;
		/** Maximum number of messages fetched per batch. */
		private int batchSize = 30;
		/** Batch fetch timeout in seconds. */
		private int batchTimeoutSeconds = 30;
		/** Timeout in seconds for processing a single batch. */
		private int batchProcessTimeoutSeconds = 60;
		/** Delay in seconds before redelivering failed messages. */
		private int redeliveryDelaySeconds = 60;
		/** Ack timeout in seconds. */
		private int ackTimeoutSeconds = 30;
		/** Whether to enable the message retry feature. */
		private boolean retry = true;
		/** Whether the retry and dead-letter suffixes are upper-case. */
		private boolean retryDLQUpperCase = false;
		/** Maximum number of redelivery attempts before dead-lettering. */
		private int maxRedeliveryCount = 128;
		/** Whether Canal messages are flattened (plain JSON) on the broker side. */
		private boolean flatMessage = false;

		/** @return return the service url. */
		public String getServiceUrl() { return serviceUrl; }
		/** @param serviceUrl set the service url. */
		public void setServiceUrl(String serviceUrl) { this.serviceUrl = serviceUrl; }
		/** @return return the role token. */
		public String getRoleToken() { return roleToken; }
		/** @param roleToken set the role token. */
		public void setRoleToken(String roleToken) { this.roleToken = roleToken; }
		/** @return return the topic. */
		public String getTopic() { return topic; }
		/** @param topic set the topic. */
		public void setTopic(String topic) { this.topic = topic; }
		/** @return return the subscript name. */
		public String getSubscriptName() { return subscriptName; }
		/** @param subscriptName set the subscript name. */
		public void setSubscriptName(String subscriptName) { this.subscriptName = subscriptName; }
		/** @return return the batch size. */
		public int getBatchSize() { return batchSize; }
		/** @param batchSize set the batch size. */
		public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
		/** @return return the batch timeout seconds. */
		public int getBatchTimeoutSeconds() { return batchTimeoutSeconds; }
		/** @param batchTimeoutSeconds set the batch timeout seconds. */
		public void setBatchTimeoutSeconds(int batchTimeoutSeconds) { this.batchTimeoutSeconds = batchTimeoutSeconds; }
		/** @return return the batch process timeout seconds. */
		public int getBatchProcessTimeoutSeconds() { return batchProcessTimeoutSeconds; }
		/** @param batchProcessTimeoutSeconds set the batch process timeout seconds. */
		public void setBatchProcessTimeoutSeconds(int batchProcessTimeoutSeconds) { this.batchProcessTimeoutSeconds = batchProcessTimeoutSeconds; }
		/** @return return the redelivery delay seconds. */
		public int getRedeliveryDelaySeconds() { return redeliveryDelaySeconds; }
		/** @param redeliveryDelaySeconds set the redelivery delay seconds. */
		public void setRedeliveryDelaySeconds(int redeliveryDelaySeconds) { this.redeliveryDelaySeconds = redeliveryDelaySeconds; }
		/** @return return the ack timeout seconds. */
		public int getAckTimeoutSeconds() { return ackTimeoutSeconds; }
		/** @param ackTimeoutSeconds set the ack timeout seconds. */
		public void setAckTimeoutSeconds(int ackTimeoutSeconds) { this.ackTimeoutSeconds = ackTimeoutSeconds; }
		/** @return return whether retry is enabled. */
		public boolean isRetry() { return retry; }
		/** @param retry set the retry. */
		public void setRetry(boolean retry) { this.retry = retry; }
		/** @return return whether retry d l q upper case is enabled. */
		public boolean isRetryDLQUpperCase() { return retryDLQUpperCase; }
		/** @param retryDLQUpperCase set the retry d l q upper case. */
		public void setRetryDLQUpperCase(boolean retryDLQUpperCase) { this.retryDLQUpperCase = retryDLQUpperCase; }
		/** @return return the max redelivery count. */
		public int getMaxRedeliveryCount() { return maxRedeliveryCount; }
		/** @param maxRedeliveryCount set the max redelivery count. */
		public void setMaxRedeliveryCount(int maxRedeliveryCount) { this.maxRedeliveryCount = maxRedeliveryCount; }
		/** @return return whether flat message is enabled. */
		public boolean isFlatMessage() { return flatMessage; }
		/** @param flatMessage set the flat message. */
		public void setFlatMessage(boolean flatMessage) { this.flatMessage = flatMessage; }

	}

	/** @return return whether enabled is enabled. */
	public boolean isEnabled() { return enabled; }
	/** @param enabled set the enabled. */
	public void setEnabled(boolean enabled) { this.enabled = enabled; }
	public List<CanalPulsarClientProperties.Instance> getInstances() { return instances; }
	/** @param instances set the instances. */
	public void setInstances(List<CanalPulsarClientProperties.Instance> instances) { this.instances = instances; }

}
