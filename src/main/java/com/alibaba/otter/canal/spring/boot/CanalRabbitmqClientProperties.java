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
/**
 * <p>Auto-configuration for CanalRabbitmqClientProperties.</p>
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
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

        /** @return return the addresses. */
        public String getAddresses() { return addresses; }
        /** @param addresses set the addresses. */
        public void setAddresses(String addresses) { this.addresses = addresses; }
        /** @return return the vhost. */
        public String getVhost() { return vhost; }
        /** @param vhost set the vhost. */
        public void setVhost(String vhost) { this.vhost = vhost; }
        /** @return return the queue name. */
        public String getQueueName() { return queueName; }
        /** @param queueName set the queue name. */
        public void setQueueName(String queueName) { this.queueName = queueName; }
        /** @return return the access key. */
        public String getAccessKey() { return accessKey; }
        /** @param accessKey set the access key. */
        public void setAccessKey(String accessKey) { this.accessKey = accessKey; }
        /** @return return the secret key. */
        public String getSecretKey() { return secretKey; }
        /** @param secretKey set the secret key. */
        public void setSecretKey(String secretKey) { this.secretKey = secretKey; }
        /** @return return the resource owner id. */
        public Long getResourceOwnerId() { return resourceOwnerId; }
        /** @param resourceOwnerId set the resource owner id. */
        public void setResourceOwnerId(Long resourceOwnerId) { this.resourceOwnerId = resourceOwnerId; }
        /** @return return the username. */
        public String getUsername() { return username; }
        /** @param username set the username. */
        public void setUsername(String username) { this.username = username; }
        /** @return return the password. */
        public String getPassword() { return password; }
        /** @param password set the password. */
        public void setPassword(String password) { this.password = password; }
        /** @return return whether flat message is enabled. */
        public boolean isFlatMessage() { return flatMessage; }
        /** @param flatMessage set the flat message. */
        public void setFlatMessage(boolean flatMessage) { this.flatMessage = flatMessage; }

    }

	/** @return return whether enabled is enabled. */
	public boolean isEnabled() { return enabled; }
	/** @param enabled set the enabled. */
	public void setEnabled(boolean enabled) { this.enabled = enabled; }
	public List<CanalRabbitmqClientProperties.Instance> getInstances() { return instances; }
	/** @param instances set the instances. */
	public void setInstances(List<CanalRabbitmqClientProperties.Instance> instances) { this.instances = instances; }

}
