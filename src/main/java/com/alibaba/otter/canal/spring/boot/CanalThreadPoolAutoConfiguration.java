package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.handler.CanalThreadUncaughtExceptionHandler;
import com.alibaba.otter.canal.protocol.CanalPacket;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Spring Boot auto-configuration for the Canal asynchronous task executor.
 * <p>
 * Registers a dedicated {@link ThreadPoolTaskExecutor} ({@code canalTaskExecutor})
 * used by the async message handlers to dispatch Canal events to user-defined
 * {@code EntryHandler} beans. Only active when {@code canal.async=true}.
 * </p>
 *
 * <h3>Configuration keys</h3>
 * <ul>
 *   <li>{@code canal.async} — must be {@code true}</li>
 *   <li>{@code canal.thread-pool.*} — thread pool sizing and rejection policy</li>
 * </ul>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@Configuration
@ConditionalOnClass({ CanalConnector.class, CanalLifeCycle.class, CanalPacket.class })
@ConditionalOnProperty(value = CanalProperties.CANAL_ASYNC, havingValue = "true")
@EnableConfigurationProperties({CanalProperties.class, CanalThreadPoolProperties.class})
public class CanalThreadPoolAutoConfiguration {

    /**
     * Creates the Canal task executor used for asynchronous message dispatch.
     * <p>The executor is configured from {@link CanalThreadPoolProperties}:
     * pool sizing, keep-alive, queue capacity, rejection policy and a thread
     * factory that installs a {@code CanalThreadUncaughtExceptionHandler}.</p>
     *
     * @param poolProperties the thread-pool configuration properties
     * @return the initialised Canal task executor
     */
    @Bean(destroyMethod = "shutdown", name = "canalTaskExecutor")
    public ThreadPoolTaskExecutor canalTaskExecutor(CanalThreadPoolProperties poolProperties) {
        BasicThreadFactory factory = new BasicThreadFactory.Builder().namingPattern("canal-execute-thread-%d")
                .uncaughtExceptionHandler(new CanalThreadUncaughtExceptionHandler()).build();
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadFactory(factory);
        executor.setCorePoolSize(poolProperties.getCorePoolSize());
        executor.setMaxPoolSize(poolProperties.getMaxPoolSize());
        executor.setQueueCapacity(poolProperties.getQueueCapacity());
        executor.setKeepAliveSeconds(Long.valueOf(poolProperties.getKeepAlive().getSeconds()).intValue());
        executor.setAllowCoreThreadTimeOut(poolProperties.isAllowCoreThreadTimeOut());
        executor.setAwaitTerminationSeconds(poolProperties.getAwaitTerminationSeconds());
        executor.setWaitForTasksToCompleteOnShutdown(poolProperties.isWaitForTasksToCompleteOnShutdown());
        executor.setThreadNamePrefix(poolProperties.getThreadNamePrefix());
        // Rejection policy:
        //   CallerRunsPolicy - run the rejected task on the caller thread
        //   AbortPolicy      - throw a RejectedExecutionException
        //   DiscardPolicy    - silently discard the rejected task
        //   DiscardOldestPolicy - discard the oldest queued task and retry
        executor.setRejectedExecutionHandler(poolProperties.getRejectedPolicy().getRejectedExecutionHandler());
        // Initialise the executor.
        executor.initialize();
        return executor;
    }

}
