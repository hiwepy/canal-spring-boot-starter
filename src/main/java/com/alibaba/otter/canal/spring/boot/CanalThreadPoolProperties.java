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

import java.time.Duration;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

/**
 *
 * @author ： <a href="https://github.com/hiwepy">hiwepy</a>
 */
@ConfigurationProperties(CanalThreadPoolProperties.PREFIX)
@Data
public class CanalThreadPoolProperties {

	public static final String PREFIX = "canal.thread-pool";

	/**
	 * Set the ThreadPoolExecutor's core pool size. Default is 1.
	 * positive.
	 */
	private int corePoolSize = 1;

	/**
	 * Set the ThreadPoolExecutor's maximum pool size. Default is the number of Processor.
	 */
	private int maxPoolSize = Runtime.getRuntime().availableProcessors();

	/**
	 * Set the capacity for the ThreadPoolExecutor's BlockingQueue. Default is Integer.MAX_VALUE.
	 * Any positive value will lead to a LinkedBlockingQueue instance; any other value will lead to a SynchronousQueue instance.
	 */
	private int queueCapacity = Integer.MAX_VALUE;

	/**
	 * Set the ThreadPoolExecutor's keep-alive time. Default is 60 seconds.
	 */
	private Duration keepAlive = Duration.ofSeconds(60);

	/**
	 * Specify whether to allow core threads to time out. This enables dynamic
	 * growing and shrinking even in combination with a non-zero queue (since
	 * the max pool size will only grow once the queue is full).
	 * <p>Default is "false".
	 */
	private boolean allowCoreThreadTimeOut = false;

	private boolean waitForTasksToCompleteOnShutdown = false;

	private int awaitTerminationSeconds = 0;

	/**
	 * Specify the prefix to use for the names of newly created threads.
	 * Default is "RedisAsyncTaskExecutor-".
	 */
	private String threadNamePrefix = "RedisAsyncTaskExecutor-";

	/**
	 * Set whether this factory is supposed to create daemon threads,
	 * just executing as long as the application itself is running.
	 * <p>Default is "false": Concrete factories usually support explicit cancelling.
	 * Hence, if the application shuts down, Runnables will by default finish their
	 * execution.
	 * <p>Specify "true" for eager shutdown of threads which still actively execute
	 * a {@link Runnable} at the time that the application itself shuts down.
	 */
	private boolean daemon = false;

	/**
	 * Set the Rejected Policy to use for the ExecutorService.
	 * Default is the ExecutorService's default abort policy.
	 * @see java.util.concurrent.ThreadPoolExecutor.AbortPolicy
	 */
	private RejectedPolicy rejectedPolicy = RejectedPolicy.AbortPolicy;


	/**
	 * 拒绝处理策略
	 * CallerRunsPolicy()：交由调用方线程运行，比如 main 线程。
	 * AbortPolicy()：直接抛出异常。
	 * DiscardPolicy()：直接丢弃。
	 * DiscardOldestPolicy()：丢弃队列中最老的任务。
	 */
	public enum RejectedPolicy {

		AbortPolicy((e) -> {
			return new ThreadPoolExecutor.AbortPolicy();
		}),
		CallerRunsPolicy((e) -> {
			return new ThreadPoolExecutor.CallerRunsPolicy();
		}),
		DiscardPolicy((e) -> {
			return new ThreadPoolExecutor.DiscardPolicy();
		}),
		DiscardOldestPolicy((e) -> {
			return new ThreadPoolExecutor.DiscardOldestPolicy();
		});

		private Function<Object, RejectedExecutionHandler> function;

		private RejectedPolicy(Function<Object, RejectedExecutionHandler> function) {
			this.function = function;
		}

		public RejectedExecutionHandler getRejectedExecutionHandler(){
			return this.function.apply(null);
		}

	}
}
