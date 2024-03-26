package com.alibaba.otter.canal.handler.impl;


import com.alibaba.otter.canal.handler.AbstractFlatMessageHandler;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.handler.RowDataHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;

import java.util.List;
import java.util.Map;

/**
 * 同步处理 FlatMessage
 */
public class SyncFlatMessageHandlerImpl extends AbstractFlatMessageHandler {

    public SyncFlatMessageHandlerImpl(List<? extends EntryHandler> entryHandlers,
                                      RowDataHandler<List<Map<String, String>>> rowDataHandler) {
        super(null, entryHandlers, rowDataHandler);
    }

    public SyncFlatMessageHandlerImpl(List<CanalEntry.EntryType> subscribeTypes,
                                      List<? extends EntryHandler> entryHandlers,
                                      RowDataHandler<List<Map<String, String>>> rowDataHandler) {
        super(subscribeTypes, entryHandlers, rowDataHandler);
    }

    @Override
    public void handleMessage(String destination, FlatMessage flatMessage) {
        super.handleMessage(destination, flatMessage);
    }
}
