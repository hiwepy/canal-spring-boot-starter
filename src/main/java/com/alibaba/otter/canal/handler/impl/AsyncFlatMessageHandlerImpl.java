package com.alibaba.otter.canal.handler.impl;


import com.alibaba.otter.canal.handler.AbstractFlatMessageHandler;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.handler.RowDataHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.List;
import java.util.Map;

/**
 * Asynchronous implementation of {@link com.alibaba.otter.canal.handler.MessageHandler}
 * for MQ-based Canal clients.
 * <p>
 * Delegates each incoming {@link FlatMessage} to the inherited
 * {@link AbstractFlatMessageHandler} logic, but runs the dispatch on a worker
 * thread drawn from the supplied {@link ThreadPoolTaskExecutor} so that the MQ
 * consumer thread is not blocked by handler work.
 * </p>
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */
public class AsyncFlatMessageHandlerImpl extends AbstractFlatMessageHandler {

    /** Executor used to dispatch messages asynchronously. */
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    /**
     * @param entryHandlers         programmatic entry handlers to register
     * @param rowDataHandler        the row data handler used to transform row data
     * @param threadPoolTaskExecutor the executor used for async dispatch
     */
    public AsyncFlatMessageHandlerImpl(List<? extends EntryHandler> entryHandlers,
                                       RowDataHandler<List<Map<String, String>>> rowDataHandler,
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
    public AsyncFlatMessageHandlerImpl(List<CanalEntry.EntryType> subscribeTypes,
                                       List<? extends EntryHandler> entryHandlers,
                                       RowDataHandler<List<Map<String, String>>> rowDataHandler,
                                       ThreadPoolTaskExecutor threadPoolTaskExecutor) {
        super(subscribeTypes, entryHandlers, rowDataHandler);
        this.threadPoolTaskExecutor = threadPoolTaskExecutor;
    }

    /**
     * Dispatches the flat message on a worker thread of the configured executor.
     *
     * @param destination the Canal destination the message originated from
     * @param flatMessage the flat message to handle
     */
    @Override
    public void handleMessage(String destination, FlatMessage flatMessage) {
        threadPoolTaskExecutor.execute(() -> super.handleMessage(destination, flatMessage));
    }


}
