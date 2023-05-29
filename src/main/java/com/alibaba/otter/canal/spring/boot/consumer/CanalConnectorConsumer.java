package com.alibaba.otter.canal.spring.boot.consumer;

import com.alibaba.otter.canal.client.CanalConnector;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ErrorHandler;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class CanalConnectorConsumer<C extends CanalConnector> {

    /**
     * Error Handler
     */
    protected ErrorHandler handler         = (e) -> log.error("parse events has an error", e);
    /**
     * Batch consumption size
     */
    protected int consumeMessageBatchMaxSize = 1;
    /**
     * Minimum consumer thread number
     */
    private int consumeThreadMin = 20;
    /**
     * Max consumer thread number
     */
    private int consumeThreadMax = 20;
    /**
     * Maximum amount of time in minutes a message may block the consuming thread.
     */
    protected long consumeTimeout = 15;

    protected Long timeout = 0L;
    protected TimeUnit unit = TimeUnit.SECONDS;
    protected boolean withoutAck;
    private List<C> connectors;
    private ThreadPoolTaskScheduler threadPoolTaskScheduler;

    public CanalConnectorConsumer(List<C> connectors){
        this.connectors = connectors;
        this.threadPoolTaskScheduler = intiThreadPoolTaskScheduler();
    }

    protected ThreadPoolTaskScheduler intiThreadPoolTaskScheduler(){
        if(CollectionUtils.isEmpty(connectors)){
            return null;
        }
        ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
        threadPoolTaskScheduler.setErrorHandler(handler);
        threadPoolTaskScheduler.setPoolSize(1);
        threadPoolTaskScheduler.setThreadFactory(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
        threadPoolTaskScheduler.initialize();
        return threadPoolTaskScheduler;
    }

    /**
     * Start consume
     */
    public synchronized void start() {
        if(CollectionUtils.isEmpty(connectors)){
            return;
        }
        for (C connector: connectors) {
            this.threadPoolTaskScheduler.scheduleAtFixedRate(() ->{
                try {
                    this.consumeMessage(connector);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, 100);
        }
    }

    /**
     * consume message
     * @param connector Canal Connector
     */
    public abstract void consumeMessage(C connector);

    /**
     * shutdown method
     */
    public void shutdown() {
        this.threadPoolTaskScheduler.shutdown();
    }

    public int getConsumeThreadMin() {
        return consumeThreadMin;
    }

    public void setConsumeThreadMin(int consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
    }

    public int getConsumeThreadMax() {
        return consumeThreadMax;
    }

    public void setConsumeThreadMax(int consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
    }

    public long getConsumeTimeout() {
        return consumeTimeout;
    }

    public void setConsumeTimeout(long consumeTimeout) {
        this.consumeTimeout = consumeTimeout;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    public void setUnit(TimeUnit unit) {
        this.unit = unit;
    }

    public boolean isWithoutAck() {
        return withoutAck;
    }

    public void setWithoutAck(boolean withoutAck) {
        this.withoutAck = withoutAck;
    }

    public int getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }

    public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }
}
