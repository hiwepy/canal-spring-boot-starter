package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.handler.MessageHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.experimental.Accessors;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Accessors(chain = true)
public abstract class AbstractClientBuilder<R extends CanalClient, C extends CanalConnector> {

    /**
     * 消息过滤
     */
    protected String filter = StringUtils.EMPTY;
    /**
     * 批处理大小
     */
    protected Integer batchSize = 1;
    /**
     * 获取数据超时时间
     */
    protected Long timeout = 1L;
    /**
     * 获取数据超时时间单位
     */
    protected TimeUnit unit = TimeUnit.SECONDS;
    /**
     * 指定订阅的事件类型，主要用于标识事务的开始，变更数据，结束
     */
    protected List<CanalEntry.EntryType> subscribeTypes = Arrays.asList(CanalEntry.EntryType.ROWDATA);
    /**
     * 消息处理器
     */
    protected MessageHandler messageHandler;

    public AbstractClientBuilder filter(String filter) {
        this.filter = filter;
        return this;
    }

    public AbstractClientBuilder batchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public AbstractClientBuilder timeout(Long timeout) {
        this.timeout = timeout;
        return this;
    }

    public AbstractClientBuilder unit(TimeUnit unit) {
        this.unit = unit;
        return this;
    }

    public AbstractClientBuilder setSubscribeTypes(List<CanalEntry.EntryType> subscribeTypes) {
        this.subscribeTypes = subscribeTypes;
        return this;
    }

    public AbstractClientBuilder messageHandler(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
        return this;
    }

    public abstract R build(List<C> connectors);

}
