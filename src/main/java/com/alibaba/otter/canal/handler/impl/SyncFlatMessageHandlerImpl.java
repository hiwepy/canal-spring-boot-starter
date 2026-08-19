package com.alibaba.otter.canal.handler.impl;


import com.alibaba.otter.canal.handler.AbstractFlatMessageHandler;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.handler.RowDataHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;

import java.util.List;
import java.util.Map;

/**
 * Synchronous implementation of {@link com.alibaba.otter.canal.handler.MessageHandler}
 * for MQ-based Canal clients.
 * <p>
 * Runs the inherited {@link AbstractFlatMessageHandler} dispatch logic on the
 * calling (MQ consumer) thread, blocking it until all handlers have been
 * invoked for the message.
 * </p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public class SyncFlatMessageHandlerImpl extends AbstractFlatMessageHandler {

    /**
     * @param entryHandlers  programmatic entry handlers to register
     * @param rowDataHandler the row data handler used to transform row data
     */
    public SyncFlatMessageHandlerImpl(List<? extends EntryHandler> entryHandlers,
                                      RowDataHandler<List<Map<String, String>>> rowDataHandler) {
        super(null, entryHandlers, rowDataHandler);
    }

    /**
     * @param subscribeTypes entry types to subscribe to
     * @param entryHandlers  programmatic entry handlers to register
     * @param rowDataHandler the row data handler used to transform row data
     */
    public SyncFlatMessageHandlerImpl(List<CanalEntry.EntryType> subscribeTypes,
                                      List<? extends EntryHandler> entryHandlers,
                                      RowDataHandler<List<Map<String, String>>> rowDataHandler) {
        super(subscribeTypes, entryHandlers, rowDataHandler);
    }

    /**
     * Handles the flat message on the calling thread.
     *
     * @param destination the Canal destination the message originated from
     * @param flatMessage the flat message to handle
     */
    @Override
    /**
     * <p>Handle message.</p>
     * @param destination
     * @param flatMessage
     */
    public void handleMessage(String destination, FlatMessage flatMessage) {
        super.handleMessage(destination, flatMessage);
    }
}
