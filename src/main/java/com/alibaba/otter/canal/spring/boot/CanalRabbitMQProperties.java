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

@ConfigurationProperties(CanalRabbitMQProperties.PREFIX)
@Data
public class CanalRabbitMQProperties {

	public static final String PREFIX = "canal.rabbitmq";
	
	/** 
	 * Whether Enable Canal RabbitMQ. 
	 */
	private boolean enabled = false;
	
	// 链接地址
    private String                              nameServer;

    // 主机名
    private String                              vhost;

    private String                              queueName;

    // 一些鉴权信息
    private String                              accessKey;
    private String                              secretKey;
    private Long                                resourceOwnerId;
    private String                              username;
    private String                              password;

    private boolean                             flatMessage;
    
    
}
