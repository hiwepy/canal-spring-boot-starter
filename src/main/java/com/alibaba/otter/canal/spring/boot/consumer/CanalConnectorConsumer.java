package com.alibaba.otter.canal.spring.boot.consumer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import com.alibaba.otter.canal.spring.boot.CanalConsumerProperties;
import com.alibaba.otter.canal.spring.boot.consumer.listener.MessageListener;
import com.alibaba.otter.canal.spring.boot.consumer.listener.MessageListenerConcurrently;
import com.alibaba.otter.canal.spring.boot.consumer.listener.MessageListenerOrderly;
import com.alibaba.otter.canal.spring.boot.exception.CanalConsumeException;
import com.alibaba.otter.canal.spring.boot.hooks.CanalConsumerHook;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ErrorHandler;

import java.util.List;
import java.util.Objects;

@Slf4j
public abstract class CanalConnectorConsumer<C extends CanalConnector, T> {

    protected PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
    /**
     * Error Handler
     */
    protected ErrorHandler handler         = (e) -> log.error("parse events has an error", e);
    private volatile boolean running     = false;

    private long awaitTerminateMillis;

    /**
     * Batch consumption size
     */
    private int consumeMessageBatchMaxSize = 1;
    /**
     * If consumer Orderly, default is false
     */
    private boolean consumeOrderly = false;
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
     * Period each consume
     */
    private long consumePeriod = 1000;
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
     * If user flat message model
     */
    private boolean flatMessage;
    /**
     * The Canal Connector List
     */
    private List<C> connectors;
    /**
     * Canal Message Consumer Scheduler
     */
    private ThreadPoolTaskScheduler threadPoolTaskScheduler;

    private ConsumeMessageService<T> consumeMessageService;
    private MessageListener messageListenerInner;

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
        this.running = true;
        for (C connector: connectors) {
            this.threadPoolTaskScheduler.scheduleAtFixedRate(() ->{
                try {
                    if (!this.running) {
                        return;
                    }
                    connector.connect();
                    if(!(connector instanceof KafkaCanalConnector)){
                        connector.subscribe(this.getConsumeFilter());
                    }
                    if (!this.running) {
                        return;
                    }
                    this.consumeMessage(connector);
                } catch (Throwable e) {
                    log.error("process error!", e);
                    // 处理失败, 回滚数据
                    connector.rollback();
                    if(CanalConsumeException.class.isAssignableFrom(e.getClass())){
                        throw e;
                    }
                    throw new CanalConsumeException(e);
                } finally {
                    connector.unsubscribe();
                    connector.disconnect();
                }
            }, this.getConsumePeriod());
        }

        if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
            this.consumeOrderly = true;
            this.consumeMessageService = new ConsumeMessageOrderlyService(this,
                    (MessageListenerOrderly) this.getMessageListenerInner());
        } else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
            this.consumeOrderly = false;
            this.consumeMessageService = new ConsumeMessageConcurrentlyService(this,
                    (MessageListenerConcurrently) this.getMessageListenerInner());
        }

        this.consumeMessageService.start();

        Runtime.getRuntime().addShutdownHook(new CanalConsumerHook(this.consumeMessageService, this.getAwaitTerminateMillis()));

    }

    /**
     * consume message
     * @param connector Canal Connector
     */
    public abstract void consumeMessage(C connector);

    public void initConsumer(CanalConsumerProperties consumerProperties){
        map.from(consumerProperties.getAwaitTerminateMillis()).to(this::setAwaitTerminateMillis);
        map.from(consumerProperties.getConsumeMessageBatchMaxSize()).to(this::setConsumeMessageBatchMaxSize);
        map.from(consumerProperties.getConsumePeriod()).to(this::setConsumePeriod);
        map.from(consumerProperties.getConsumeTimeout()).to(this::setConsumeTimeout);
        map.from(consumerProperties.getConsumeThreadMax()).to(this::setConsumeThreadMax);
        map.from(consumerProperties.getConsumeThreadMin()).to(this::setConsumeThreadMin);
        map.from(consumerProperties.getConsumeFilter()).to(this::setConsumeFilter);
        map.from(consumerProperties.getBatchSize()).to(this::setBatchSize);
        map.from(consumerProperties.getReadTimeout()).to(this::setReadTimeout);
        map.from(consumerProperties.isRequireAck()).to(this::setRequireAck);
        map.from(consumerProperties.isFlatMessage()).to(this::setFlatMessage);
    }

    /**
     * shutdown method
     */
    public void shutdown() {
        if (!this.running) {
            return;
        }
        this.running = false;
        this.threadPoolTaskScheduler.shutdown();
        for (C connector: connectors) {
            try {
                connector.unsubscribe();
                connector.disconnect();
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
        if(Objects.nonNull( this.consumeMessageService)){
            this.consumeMessageService.shutdown(awaitTerminateMillis);
        }
    }

    public void registerMessageListener(MessageListener messageListener) {
        this.messageListenerInner = messageListener;
    }

    public MessageListener getMessageListenerInner() {
        return messageListenerInner;
    }

    public ConsumeMessageService getConsumeMessageService() {
        return consumeMessageService;
    }

    public long getAwaitTerminateMillis() {
        return awaitTerminateMillis;
    }

    public void setAwaitTerminateMillis(long awaitTerminateMillis) {
        this.awaitTerminateMillis = awaitTerminateMillis;
    }

    public boolean isConsumeOrderly() {
        return consumeOrderly;
    }

    public void setConsumeOrderly(boolean consumeOrderly) {
        this.consumeOrderly = consumeOrderly;
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

    public long getConsumePeriod() {
        return consumePeriod;
    }

    public void setConsumePeriod(long consumePeriod) {
        this.consumePeriod = consumePeriod;
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

    public boolean isFlatMessage() {
        return flatMessage;
    }

    public void setFlatMessage(boolean flatMessage) {
        this.flatMessage = flatMessage;
    }

    public String getConsumeFilter() {
        return consumeFilter;
    }

    public void setConsumeFilter(String consumeFilter) {
        this.consumeFilter = consumeFilter;
    }

}
