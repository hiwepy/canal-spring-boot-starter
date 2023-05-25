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

@ConfigurationProperties(CanalRocketMQProperties.PREFIX)
@Data
public class CanalRocketMQProperties {

	public static final String PREFIX = "canal.rocketmq";
	
	/** 
	 * Whether Enable Canal RocketMQ. 
	 */
	private boolean enabled = false;
	
	// 链接地址
    private String                              nameServer;

    // 主机名
    private String                              topic;

    private String                              groupName;
    private boolean                             enableMessageTrace;

    // 一些鉴权信息
    private String                              accessKey;
    private String                              secretKey;
    private String                              accessChannel;
    private String                              namespace;

    private boolean                             flatMessage;
    private String                              customizedTraceTopic;
    Integer batchSize							= -1;
    
    
    
}
