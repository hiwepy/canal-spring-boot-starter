package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.handler.MessageHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Fluent builder base for constructing {@link CanalClient} instances.
 * <p>
 * Provides the common subscription options (filter, batch size, timeout,
 * entry types) and message handler that are applied to the client built by
 * {@link #build(List)}. Subclasses implement {@link #build(List)} to produce a
 * concrete client type from a list of connectors.
 * </p>
 *
 * @param <R> the {@link CanalClient} type produced by this builder
 * @param <C> the {@link CanalConnector} type consumed by the produced client
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */
public abstract class AbstractClientBuilder<R extends CanalClient, C extends CanalConnector> {

    /** Canal subscription filter expression. */
    protected String filter = StringUtils.EMPTY;
    /** Number of messages fetched per poll. */
    protected Integer batchSize = 1;
    /** Polling timeout. */
    protected Long timeout = 1L;
    /** Time unit applied to {@link #timeout}. */
    protected TimeUnit unit = TimeUnit.SECONDS;
    /** Entry types to subscribe to, marking transaction begin, data change and transaction end. */
    protected List<CanalEntry.EntryType> subscribeTypes = Arrays.asList(CanalEntry.EntryType.ROWDATA);
    /** Handler that receives each polled Canal message. */
    protected MessageHandler messageHandler;

    /**
     * @param filter Canal subscription filter expression
     * @return this builder for chaining
     */
    public AbstractClientBuilder filter(String filter) {
        this.filter = filter;
        return this;
    }

    /**
     * @param batchSize number of messages fetched per poll
     * @return this builder for chaining
     */
    public AbstractClientBuilder batchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * @param timeout polling timeout
     * @return this builder for chaining
     */
    public AbstractClientBuilder timeout(Long timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * @param unit time unit applied to {@link #timeout}
     * @return this builder for chaining
     */
    public AbstractClientBuilder unit(TimeUnit unit) {
        this.unit = unit;
        return this;
    }

    /**
     * @param subscribeTypes entry types to subscribe to
     * @return this builder for chaining
     */
    public AbstractClientBuilder setSubscribeTypes(List<CanalEntry.EntryType> subscribeTypes) {
        this.subscribeTypes = subscribeTypes;
        return this;
    }

    /**
     * @param messageHandler handler that receives each polled message
     * @return this builder for chaining
     */
    public AbstractClientBuilder messageHandler(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
        return this;
    }

    /**
     * Builds a Canal client that consumes from the supplied connectors.
     *
     * @param connectors the connectors the client will consume from
     * @return the constructed client
     */
    public abstract R build(List<C> connectors);

}
