package com.alibaba.otter.canal.handler.impl;


import com.alibaba.otter.canal.handler.AbstractMessageHandler;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.handler.RowDataHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import java.util.List;

/**
 * Synchronous implementation of {@link com.alibaba.otter.canal.handler.MessageHandler}
 * for direct (non-MQ) Canal clients.
 * <p>
 * Runs the inherited {@link AbstractMessageHandler} dispatch logic on the
 * calling (Canal polling) thread, blocking it until all handlers have been
 * invoked for the message.
 * </p>
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */
public class SyncMessageHandlerImpl extends AbstractMessageHandler {


    /**
     * @param entryHandlers  programmatic entry handlers to register
     * @param rowDataHandler the row data handler used to transform row data
     */
    public SyncMessageHandlerImpl(List<? extends EntryHandler> entryHandlers,
                                  RowDataHandler<CanalEntry.RowData> rowDataHandler) {
        super(null, entryHandlers, rowDataHandler);
    }

    /**
     * @param subscribeTypes entry types to subscribe to
     * @param entryHandlers  programmatic entry handlers to register
     * @param rowDataHandler the row data handler used to transform row data
     */
    public SyncMessageHandlerImpl(List<CanalEntry.EntryType> subscribeTypes,
                                  List<? extends EntryHandler> entryHandlers,
                                  RowDataHandler<CanalEntry.RowData> rowDataHandler) {
        super(subscribeTypes, entryHandlers, rowDataHandler);
    }

    /**
     * Handles the message on the calling thread.
     *
     * @param destination the Canal destination the message originated from
     * @param message     the protobuf message to handle
     */
    @Override
    public void handleMessage(String destination, Message message) {
        super.handleMessage(destination, message);
    }


}
