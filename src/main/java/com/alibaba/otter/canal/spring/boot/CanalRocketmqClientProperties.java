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

@ConfigurationProperties(CanalRocketmqClientProperties.PREFIX)
@Data
public class CanalRocketmqClientProperties {

	public static final String PREFIX = "canal.rocketmq";

	/**
	 * Whether Enable Canal RocketMQ.
	 */
	private boolean enabled = false;

    /**
     * 配置信息
     */
    private List<CanalRocketmqClientProperties.Instance> instances = new ArrayList<>();

    @Data
    public static class Instance {

        /**
         * RocketMQ NameServer 服务地址
         */
        private String                              nameServer;
        /**
         * 订阅的消息主题
         */
        private String                              topic;
        /**
         * 消费者组名称
         */
        private String                              groupName;
        /**
         * 是否开启消息轨迹
         */
        private boolean                             enableMessageTrace;
        /**
         * 访问Key
         */
        private String                              accessKey;
        /**
         * 访问密钥
         */
        private String                              secretKey;
        /**
         * 访问的通道
         */
        private String                              accessChannel;
        /**
         * 命名空间
         */
        private String                              namespace;
        /**
         * 自定义轨迹主题
         */
        private String                              customizedTraceTopic;
        /**
         * 批量获取数据的大小
         */
        private Integer batchSize					= -1;

    }

}
