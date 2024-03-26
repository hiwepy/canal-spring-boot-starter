package com.alibaba.otter.canal.handler.impl;


import com.alibaba.otter.canal.handler.AbstractMessageHandler;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.handler.RowDataHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import java.util.List;

/**
 * @author yang peng
 * @date 2019/3/2710:52
 */
public class SyncMessageHandlerImpl extends AbstractMessageHandler {


    public SyncMessageHandlerImpl(List<? extends EntryHandler> entryHandlers,
                                  RowDataHandler<CanalEntry.RowData> rowDataHandler) {
        super(null, entryHandlers, rowDataHandler);
    }

    public SyncMessageHandlerImpl(List<CanalEntry.EntryType> subscribeTypes,
                                  List<? extends EntryHandler> entryHandlers,
                                  RowDataHandler<CanalEntry.RowData> rowDataHandler) {
        super(subscribeTypes, entryHandlers, rowDataHandler);
    }

    @Override
    public void handleMessage(String destination, Message message) {
        super.handleMessage(destination, message);
    }


}
