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
 * Connection properties for the Canal <strong>RabbitMQ</strong> client mode.
 * <p>
 * Bound to the {@code canal.rabbitmq.*} configuration namespace. Each
 * {@link Instance} describes a RabbitMQ consumer subscribing to Canal binlog
 * events published to a RabbitMQ queue.
 * </p>
 *
 * <h3>Configuration keys</h3>
 * <ul>
 *   <li>{@code canal.rabbitmq.enabled} — whether the RabbitMQ client is enabled (default {@code false})</li>
 *   <li>{@code canal.rabbitmq.instances} — list of RabbitMQ Canal consumer definitions</li>
 * </ul>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@ConfigurationProperties(CanalRabbitmqClientProperties.PREFIX)
public class CanalRabbitmqClientProperties {

	/** Configuration prefix used by Spring Boot to bind properties. */
	public static final String PREFIX = "canal.rabbitmq";

	/**
	 * Whether to enable the Canal RabbitMQ client. When {@code false} (the
	 * default) the RabbitMQ auto-configuration is skipped.
	 */
	private boolean enabled = false;

    /**
     * List of RabbitMQ Canal consumer connection definitions. Each entry
     * produces one {@code RabbitMQCanalConnector} when the RabbitMQ client is built.
     */
    private List<CanalRabbitmqClientProperties.Instance> instances = new ArrayList<>();

    /**
     * Connection definition for a single RabbitMQ Canal consumer.
     */
    public static class Instance {

        /** Comma-separated RabbitMQ broker server addresses. */
        private String addresses;
        /** RabbitMQ virtual host. */
        private String vhost;
        /** RabbitMQ queue name that Canal publishes binlog events to. */
        private String queueName;
        /** Alibaba Cloud access key, when connecting to Cloud RabbitMQ. */
        private String accessKey;
        /** Alibaba Cloud secret key, when connecting to Cloud RabbitMQ. */
        private String secretKey;
        /** Alibaba Cloud resource owner id, when connecting to Cloud RabbitMQ. */
        private Long resourceOwnerId;
        /** RabbitMQ username, if authentication is enabled. */
        private String username;
        /** RabbitMQ password, if authentication is enabled. */
        private String password;
        /** Whether Canal messages are flattened (plain JSON) on the broker side. */
        private boolean flatMessage;

        public String getAddresses() { return addresses; }
        public void setAddresses(String addresses) { this.addresses = addresses; }
        public String getVhost() { return vhost; }
        public void setVhost(String vhost) { this.vhost = vhost; }
        public String getQueueName() { return queueName; }
        public void setQueueName(String queueName) { this.queueName = queueName; }
        public String getAccessKey() { return accessKey; }
        public void setAccessKey(String accessKey) { this.accessKey = accessKey; }
        public String getSecretKey() { return secretKey; }
        public void setSecretKey(String secretKey) { this.secretKey = secretKey; }
        public Long getResourceOwnerId() { return resourceOwnerId; }
        public void setResourceOwnerId(Long resourceOwnerId) { this.resourceOwnerId = resourceOwnerId; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public boolean isFlatMessage() { return flatMessage; }
        public void setFlatMessage(boolean flatMessage) { this.flatMessage = flatMessage; }

    }

	public boolean isEnabled() { return enabled; }
	public void setEnabled(boolean enabled) { this.enabled = enabled; }
	public List<CanalRabbitmqClientProperties.Instance> getInstances() { return instances; }
	public void setInstances(List<CanalRabbitmqClientProperties.Instance> instances) { this.instances = instances; }

}
