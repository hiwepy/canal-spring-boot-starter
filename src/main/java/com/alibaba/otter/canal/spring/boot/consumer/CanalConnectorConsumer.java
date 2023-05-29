package com.alibaba.otter.canal.spring.boot.consumer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.spring.boot.CanalConsumerProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ErrorHandler;

import java.util.List;

@Slf4j
public abstract class CanalConnectorConsumer<C extends CanalConnector> {

    protected PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
    /**
     * Error Handler
     */
    protected ErrorHandler handler         = (e) -> log.error("parse events has an error", e);
    /**
     * Batch consumption size
     */
    private int consumeMessageBatchMaxSize = 1;
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
    private long consumeTimeout = 15;
    /**
     *  The timeout for reading batchSize records
     */
    private Integer batchSize = 1000;
    /**
     *  The timeout for reading batchSize records, If timeout=0, block until the batchSize record is obtained before returning
     */
    private Long readTimeout = 0L;
    /**
     * If Ack required
     */
    private boolean requireAck;
    /**
     * 客户端订阅，重复订阅时会更新对应的filter信息
     *
     * <pre>
     * 说明：
     * a. 如果本次订阅中filter信息为空，则直接使用canal server服务端配置的filter信息
     * b. 如果本次订阅中filter信息不为空，目前会直接替换canal server服务端配置的filter信息，以本次提交的为准
     * </pre>
     */
    private String consumeFilter;
    /**
     * The Canal Connector List
     */
    private List<C> connectors;
    /**
     * Canal Message Consumer Scheduler
     */
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
        threadPoolTaskScheduler.setPoolSize(Math.max(1, connectors.size()));
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

    public void initConsumer(CanalConsumerProperties consumerProperties){
        map.from(consumerProperties.getConsumeMessageBatchMaxSize()).to(this::setConsumeMessageBatchMaxSize);
        map.from(consumerProperties.getConsumeTimeout()).to(this::setConsumeTimeout);
        map.from(consumerProperties.getConsumeThreadMax()).to(this::setConsumeThreadMax);
        map.from(consumerProperties.getConsumeThreadMin()).to(this::setConsumeThreadMin);
        map.from(consumerProperties.getConsumeFilter()).to(this::setConsumeFilter);
        map.from(consumerProperties.getBatchSize()).to(this::setBatchSize);
        map.from(consumerProperties.getReadTimeout()).to(this::setReadTimeout);
        map.from(consumerProperties.isRequireAck()).to(this::setRequireAck);
    }

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

    public int getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }

    public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public Long getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(Long readTimeout) {
        this.readTimeout = readTimeout;
    }

    public boolean isRequireAck() {
        return requireAck;
    }

    public void setRequireAck(boolean requireAck) {
        this.requireAck = requireAck;
    }

    public String getConsumeFilter() {
        return consumeFilter;
    }

    public void setConsumeFilter(String consumeFilter) {
        this.consumeFilter = consumeFilter;
    }

}
