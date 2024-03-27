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

@Configuration
@ConditionalOnClass({ CanalConnector.class, CanalLifeCycle.class, CanalPacket.class })
@ConditionalOnProperty(value = CanalProperties.CANAL_ASYNC, havingValue = "true")
@EnableConfigurationProperties({CanalProperties.class, CanalThreadPoolProperties.class})
public class CanalThreadPoolAutoConfiguration {

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
        /**
         * 拒绝处理策略
         * CallerRunsPolicy()：交由调用方线程运行，比如 main 线程。
         * AbortPolicy()：直接抛出异常。
         * DiscardPolicy()：直接丢弃。
         * DiscardOldestPolicy()：丢弃队列中最老的任务。
         */
        executor.setRejectedExecutionHandler(poolProperties.getRejectedPolicy().getRejectedExecutionHandler());
        // 线程初始化
        executor.initialize();
        return executor;
    }

}
