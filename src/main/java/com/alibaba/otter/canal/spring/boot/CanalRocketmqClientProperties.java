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
 * Connection properties for the Canal <strong>RocketMQ</strong> client mode.
 * <p>
 * Bound to the {@code canal.rocketmq.*} configuration namespace. Each
 * {@link Instance} describes a RocketMQ consumer subscribing to Canal binlog
 * events published to a RocketMQ topic.
 * </p>
 *
 * <h3>Configuration keys</h3>
 * <ul>
 *   <li>{@code canal.rocketmq.enabled} — whether the RocketMQ client is enabled (default {@code false})</li>
 *   <li>{@code canal.rocketmq.instances} — list of RocketMQ Canal consumer definitions</li>
 * </ul>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@ConfigurationProperties(CanalRocketmqClientProperties.PREFIX)
public class CanalRocketmqClientProperties {

	/** Configuration prefix used by Spring Boot to bind properties. */
	public static final String PREFIX = "canal.rocketmq";

	/**
	 * Whether to enable the Canal RocketMQ client. When {@code false} (the
	 * default) the RocketMQ auto-configuration is skipped.
	 */
	private boolean enabled = false;

    /**
     * List of RocketMQ Canal consumer connection definitions. Each entry
     * produces one {@code RocketMQCanalConnector} when the RocketMQ client is built.
     */
    private List<CanalRocketmqClientProperties.Instance> instances = new ArrayList<>();

    /**
     * Connection definition for a single RocketMQ Canal consumer.
     */
    public static class Instance {

        /** RocketMQ NameServer address list. */
        private String nameServer;
        /** RocketMQ topic that Canal publishes binlog events to. */
        private String topic;
        /** RocketMQ consumer group name. */
        private String groupName;
        /** Whether to enable RocketMQ message tracing. */
        private boolean enableMessageTrace;
        /** Alibaba Cloud access key, when connecting to Cloud RocketMQ. */
        private String accessKey;
        /** Alibaba Cloud secret key, when connecting to Cloud RocketMQ. */
        private String secretKey;
        /** Alibaba Cloud access channel, when connecting to Cloud RocketMQ. */
        private String accessChannel;
        /** RocketMQ namespace, when applicable. */
        private String namespace;
        /** Custom RocketMQ message-trace topic name. */
        private String customizedTraceTopic;
        /** Number of messages fetched per batch. */
        private Integer batchSize = -1;

        public String getNameServer() { return nameServer; }
        public void setNameServer(String nameServer) { this.nameServer = nameServer; }
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public String getGroupName() { return groupName; }
        public void setGroupName(String groupName) { this.groupName = groupName; }
        public boolean isEnableMessageTrace() { return enableMessageTrace; }
        public void setEnableMessageTrace(boolean enableMessageTrace) { this.enableMessageTrace = enableMessageTrace; }
        public String getAccessKey() { return accessKey; }
        public void setAccessKey(String accessKey) { this.accessKey = accessKey; }
        public String getSecretKey() { return secretKey; }
        public void setSecretKey(String secretKey) { this.secretKey = secretKey; }
        public String getAccessChannel() { return accessChannel; }
        public void setAccessChannel(String accessChannel) { this.accessChannel = accessChannel; }
        public String getNamespace() { return namespace; }
        public void setNamespace(String namespace) { this.namespace = namespace; }
        public String getCustomizedTraceTopic() { return customizedTraceTopic; }
        public void setCustomizedTraceTopic(String customizedTraceTopic) { this.customizedTraceTopic = customizedTraceTopic; }
        public Integer getBatchSize() { return batchSize; }
        public void setBatchSize(Integer batchSize) { this.batchSize = batchSize; }

    }

	public boolean isEnabled() { return enabled; }
	public void setEnabled(boolean enabled) { this.enabled = enabled; }
	public List<CanalRocketmqClientProperties.Instance> getInstances() { return instances; }
	public void setInstances(List<CanalRocketmqClientProperties.Instance> instances) { this.instances = instances; }

}
