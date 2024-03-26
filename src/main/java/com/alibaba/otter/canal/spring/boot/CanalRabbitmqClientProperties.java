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

@ConfigurationProperties(CanalRabbitmqClientProperties.PREFIX)
@Data
public class CanalRabbitmqClientProperties {

	public static final String PREFIX = "canal.rabbitmq";

	/**
	 * Whether Enable Canal RabbitMQ.
	 */
	private boolean enabled = false;

    /**
     * 配置信息
     */
    private List<CanalRabbitmqClientProperties.Instance> instances = new ArrayList<>();

    @Data
    public static class Instance {

        /**
         * RabbitMQ服务器地址
         */
        private String                              addresses;
        /**
         * 虚拟主机
         */
        private String                              vhost;
        /**
         * 队列名称
         */
        private String                              queueName;
        /**
         * 访问Key
         */
        private String                              accessKey;
        /**
         * 访问密钥
         */
        private String                              secretKey;
        /**
         * 资源所有者的ID
         */
        private Long                                resourceOwnerId;
        /**
         * 用户名
         */
        private String                              username;
        /**
         * 密码
         */
        private String                              password;
        /**
         * 是否扁平化Canal消息内容
         */
        private boolean                             flatMessage;

    }

}
