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

import com.alibaba.otter.canal.spring.boot.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Canal 集成 Disruptor ， 作为消费者
 * @author ： <a href="https://github.com/hiwepy">hiwepy</a>
 */
@ConfigurationProperties(CanalDisruptorProperties.PREFIX)
@Data
public class CanalDisruptorProperties {

    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_MAX_SLEEP_MS = Integer.MAX_VALUE;

	public static final String PREFIX = "canal.disruptor";

	/**
	 * RingBuffer缓冲区大小, 默认 1024
	 */
	private int ringBufferSize = 1024;
	/**
	 * 消息消费线程池大小, 默认 4
	 * */
	private int ringThreadNumbers = 4;
	/**
	 * 是否对生产者，如果是则通过 RingBuffer.createMultiProducer创建一个多生产者的RingBuffer，否则通过RingBuffer.createSingleProducer创建一个单生产者的RingBuffer
	 * */
	private ProducerType producerType = ProducerType.SINGLE;
	/**
	 * 消费者等待生产者将Event置入Disruptor的策略。用来权衡当生产者无法将新的事件放进RingBuffer时的处理策略。
	 * （例如：当生产者太快，消费者太慢，会导致生成者获取不到新的事件槽来插入新事件，则会根据该策略进行处理，默认会堵塞）
	 * BLOCKING_WAIT：是最低效的策略，但其对CPU的消耗最小并且在各种不同部署环境中能提供更加一致的性能表现
	 * SLEEPING_WAIT：性能表现跟BlockingWaitStrategy差不多，对CPU的消耗也类似，但其对生产者线程的影响最小，适合用于异步日志类似的场景
	 * YIELDING_WAIT：可以被用在低延迟系统中的两个策略之一，这种策略在减低系统延迟的同时也会增加CPU运算量。YieldingWaitStrategy策略会循环等待sequence增加到合适的值。循环中调用Thread.yield()允许其他准备好的线程执行。
	 * 	如果需要高性能而且事件消费者线程比逻辑内核少的时候，推荐使用YieldingWaitStrategy策略。例如：在开启超线程的时候。
	 * BUSYSPIN_WAIT：性能最高的等待策略，同时也是对部署环境要求最高的策略。这个性能最好用在事件处理线程比物理内核数目还要小的时候。例如：在禁用超线程技术的时候。
	 */
	private WaitStrategy waitStrategy = WaitStrategy.BLOCKING_WAIT;

}
