package com.alibaba.otter.canal.handler.impl;


import com.alibaba.otter.canal.handler.AbstractMessageHandler;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.handler.RowDataHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.List;

/**
 * Asynchronous implementation of {@link com.alibaba.otter.canal.handler.MessageHandler}
 * for direct (non-MQ) Canal clients.
 * <p>
 * Delegates each incoming protobuf {@link Message} to the inherited
 * {@link AbstractMessageHandler} logic, but runs the dispatch on a worker
 * thread drawn from the supplied {@link ThreadPoolTaskExecutor} so that the
 * Canal polling thread is not blocked by handler work.
 * </p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public class AsyncMessageHandlerImpl extends AbstractMessageHandler {

    /** Executor used to dispatch messages asynchronously. */
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    /**
     * @param entryHandlers         programmatic entry handlers to register
     * @param rowDataHandler        the row data handler used to transform row data
     * @param threadPoolTaskExecutor the executor used for async dispatch
     */
    public AsyncMessageHandlerImpl(List<? extends EntryHandler> entryHandlers,
                                   RowDataHandler<CanalEntry.RowData> rowDataHandler,
                                   ThreadPoolTaskExecutor threadPoolTaskExecutor) {
        super(null, entryHandlers, rowDataHandler);
        this.threadPoolTaskExecutor = threadPoolTaskExecutor;
    }

    /**
     * @param subscribeTypes        entry types to subscribe to
     * @param entryHandlers         programmatic entry handlers to register
     * @param rowDataHandler        the row data handler used to transform row data
     * @param threadPoolTaskExecutor the executor used for async dispatch
     */
    public AsyncMessageHandlerImpl(List<CanalEntry.EntryType> subscribeTypes,
                                   List<? extends EntryHandler> entryHandlers,
                                   RowDataHandler<CanalEntry.RowData> rowDataHandler,
                                   ThreadPoolTaskExecutor threadPoolTaskExecutor) {
        super(subscribeTypes, entryHandlers, rowDataHandler);
        this.threadPoolTaskExecutor = threadPoolTaskExecutor;
    }

    /**
     * Dispatches the message on a worker thread of the configured executor.
     *
     * @param destination the Canal destination the message originated from
     * @param message     the protobuf message to handle
     */
    @Override
    /**
     * <p>Handle message.</p>
     * @param destination
     * @param message
     */
    public void handleMessage(String destination, Message message) {
        threadPoolTaskExecutor.execute(() -> super.handleMessage(destination, message));
    }

}
