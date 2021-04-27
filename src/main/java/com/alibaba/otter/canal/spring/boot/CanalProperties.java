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

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.lmax.disruptor.dsl.ProducerType;

import lombok.Data;

/**
 * https://www.ishumei.com/
 * 
 * @author ： <a href="https://github.com/hiwepy">hiwepy</a>
 */
@ConfigurationProperties(CanalProperties.PREFIX)
@Data
public class CanalProperties {

	public static final String PREFIX = "canal";
	
	/**
	 * Allows to configure if the ensemble configuration changes will be watched.
         * The default value is {@code true}.
	 */
	private boolean withEnsembleTracker = true;
	
	/**
	 * 会话超时时间（单位：毫秒），默认 30000
	 */
	private int sessionTimeoutMs = 30000;
	/**
	 * 连接超时时间（单位：毫秒），默认 3000
	 */
	private int connectionTimeoutMs = 3000;

	private DisruptorProperties disruptor;
	
	
	@Data
	public class DisruptorProperties {
		
		/** RingBuffer缓冲区大小, 默认 1024 */
		private int ringBufferSize = 1024;
		/** 消息消费线程池大小, 默认 4 */
		private int ringThreadNumbers = 4;
		/** 是否对生产者，如果是则通过 RingBuffer.createMultiProducer创建一个多生产者的RingBuffer，否则通过RingBuffer.createSingleProducer创建一个单生产者的RingBuffer */
		private ProducerType producerType = ProducerType.SINGLE;
		
	}

}
