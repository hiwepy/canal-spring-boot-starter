package com.alibaba.otter.canal.spring.boot.consumer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.spring.boot.consumer.listener.MessageListenerConcurrently;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class CanalConnectorFixedRateConsumer {

    /**
     * Error Handler
     */
    protected ErrorHandler handler         = (e) -> log.error("parse events has an error", e);
    /**
     * Batch consumption size
     */
    private int consumeMessageBatchMaxSize = 1;
    /**
     * Maximum amount of time in minutes a message may block the consuming thread.
     */
    private long consumeTimeout = 15;

    protected Long timeout = 0L;
    protected TimeUnit unit = TimeUnit.SECONDS;
    protected boolean withoutAck;
    protected List<CanalConnector> connectors;
    private final ThreadPoolTaskScheduler threadPoolTaskScheduler;

    public CanalConnectorFixedRateConsumer(List<CanalConnector> connectors, MessageListenerConcurrently messageListener){
        Assert.notNull(connectors, "connectors is null");
        this.connectors = connectors;
        this.threadPoolTaskScheduler = intiThreadPoolTaskScheduler();
    }

    protected ThreadPoolTaskScheduler intiThreadPoolTaskScheduler(){
        ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
        threadPoolTaskScheduler.setErrorHandler(handler);
        threadPoolTaskScheduler.setPoolSize(1);
        threadPoolTaskScheduler.setThreadFactory(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
        threadPoolTaskScheduler.initialize();
        threadPoolTaskScheduler.scheduleAtFixedRate(() ->{

        }, 100);
        return threadPoolTaskScheduler;
    }

    void start() {

    }

    void shutdown(long awaitTerminateMillis) {

    }

}
