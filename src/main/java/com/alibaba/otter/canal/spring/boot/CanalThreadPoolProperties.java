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

import java.time.Duration;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

/**
 * Thread-pool configuration for the Canal asynchronous task executor.
 * <p>
 * Bound to the {@code canal.thread-pool.*} configuration namespace. Controls
 * pool sizing, queue capacity, keep-alive, naming and rejection policy of the
 * {@code canalTaskExecutor} used to dispatch Canal messages to entry handlers.
 * </p>
 *
 * <h3>Configuration keys</h3>
 * <ul>
 *   <li>{@code canal.thread-pool.core-pool-size} — core pool size (default {@code 1})</li>
 *   <li>{@code canal.thread-pool.max-pool-size} — maximum pool size (default: available processors)</li>
 *   <li>{@code canal.thread-pool.queue-capacity} — blocking queue capacity (default {@code Integer.MAX_VALUE})</li>
 *   <li>{@code canal.thread-pool.keep-alive} — keep-alive duration (default {@code 60s})</li>
 *   <li>{@code canal.thread-pool.rejected-policy} — rejection policy (default {@code AbortPolicy})</li>
 * </ul>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@ConfigurationProperties(CanalThreadPoolProperties.PREFIX)
public class CanalThreadPoolProperties {

	/** Configuration prefix used by Spring Boot to bind properties. */
	public static final String PREFIX = "canal.thread-pool";

	/**
	 * Core pool size for the Canal task executor. Must be positive. Defaults to {@code 1}.
	 */
	private int corePoolSize = 1;

	/**
	 * Maximum pool size for the Canal task executor. Defaults to the number of
	 * available processors.
	 */
	private int maxPoolSize = Runtime.getRuntime().availableProcessors();

	/**
	 * Capacity of the executor's blocking queue. Any positive value yields a
	 * {@code LinkedBlockingQueue}; any other value yields a
	 * {@code SynchronousQueue}. Defaults to {@code Integer.MAX_VALUE}.
	 */
	private int queueCapacity = Integer.MAX_VALUE;

	/**
	 * Keep-alive duration for idle threads beyond the core pool. Defaults to {@code 60s}.
	 */
	private Duration keepAlive = Duration.ofSeconds(60);

	/**
	 * Whether core threads are allowed to time out, enabling dynamic growing and
	 * shrinking even with a non-zero queue. Defaults to {@code false}.
	 */
	private boolean allowCoreThreadTimeOut = false;

	/**
	 * Whether to wait for queued tasks to complete on application shutdown.
	 * Defaults to {@code false}.
	 */
	private boolean waitForTasksToCompleteOnShutdown = false;

	/**
	 * Seconds to wait for remaining tasks to finish on shutdown before the
	 * executor is forcibly terminated. Defaults to {@code 0}.
	 */
	private int awaitTerminationSeconds = 0;

	/**
	 * Name prefix used for newly created threads. Defaults to
	 * {@code "RedisAsyncTaskExecutor-"}.
	 */
	private String threadNamePrefix = "RedisAsyncTaskExecutor-";

	/**
	 * Whether created threads should be daemon threads. Daemon threads exit when
	 * the JVM shuts down. Defaults to {@code false}.
	 */
	private boolean daemon = false;

	/**
	 * Rejection policy applied when the executor cannot accept a new task.
	 * Defaults to {@link RejectedPolicy#AbortPolicy}.
	 * @see java.util.concurrent.ThreadPoolExecutor.AbortPolicy
	 */
	private RejectedPolicy rejectedPolicy = RejectedPolicy.AbortPolicy;


	/**
	 * Rejection policies for the Canal task executor.
	 * <ul>
	 *   <li>{@link #CallerRunsPolicy} — run the rejected task on the caller thread</li>
	 *   <li>{@link #AbortPolicy} — throw a {@code RejectedExecutionException}</li>
	 *   <li>{@link #DiscardPolicy} — silently discard the rejected task</li>
	 *   <li>{@link #DiscardOldestPolicy} — discard the oldest queued task and retry</li>
	 * </ul>
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

		/** Factory that produces the matching {@link RejectedExecutionHandler}. */
		private Function<Object, RejectedExecutionHandler> function;

		private RejectedPolicy(Function<Object, RejectedExecutionHandler> function) {
			this.function = function;
		}

		/**
		 * @return the {@link RejectedExecutionHandler} associated with this policy.
		 */
		public RejectedExecutionHandler getRejectedExecutionHandler(){
			return this.function.apply(null);
		}

	}

	public int getCorePoolSize() { return corePoolSize; }
	public void setCorePoolSize(int corePoolSize) { this.corePoolSize = corePoolSize; }
	public int getMaxPoolSize() { return maxPoolSize; }
	public void setMaxPoolSize(int maxPoolSize) { this.maxPoolSize = maxPoolSize; }
	public int getQueueCapacity() { return queueCapacity; }
	public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
	public Duration getKeepAlive() { return keepAlive; }
	public void setKeepAlive(Duration keepAlive) { this.keepAlive = keepAlive; }
	public boolean isAllowCoreThreadTimeOut() { return allowCoreThreadTimeOut; }
	public void setAllowCoreThreadTimeOut(boolean allowCoreThreadTimeOut) { this.allowCoreThreadTimeOut = allowCoreThreadTimeOut; }
	public boolean isWaitForTasksToCompleteOnShutdown() { return waitForTasksToCompleteOnShutdown; }
	public void setWaitForTasksToCompleteOnShutdown(boolean waitForTasksToCompleteOnShutdown) { this.waitForTasksToCompleteOnShutdown = waitForTasksToCompleteOnShutdown; }
	public int getAwaitTerminationSeconds() { return awaitTerminationSeconds; }
	public void setAwaitTerminationSeconds(int awaitTerminationSeconds) { this.awaitTerminationSeconds = awaitTerminationSeconds; }
	public String getThreadNamePrefix() { return threadNamePrefix; }
	public void setThreadNamePrefix(String threadNamePrefix) { this.threadNamePrefix = threadNamePrefix; }
	public boolean isDaemon() { return daemon; }
	public void setDaemon(boolean daemon) { this.daemon = daemon; }
	public RejectedPolicy getRejectedPolicy() { return rejectedPolicy; }
	public void setRejectedPolicy(RejectedPolicy rejectedPolicy) { this.rejectedPolicy = rejectedPolicy; }

}
