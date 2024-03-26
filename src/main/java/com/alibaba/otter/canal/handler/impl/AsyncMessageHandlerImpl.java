package com.alibaba.otter.canal.handler.impl;


import com.alibaba.otter.canal.handler.AbstractMessageHandler;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.handler.RowDataHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.List;

/**
 * @author yang peng
 * @date 2019/3/2921:40
 */
public class AsyncMessageHandlerImpl extends AbstractMessageHandler {

    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    public AsyncMessageHandlerImpl(List<? extends EntryHandler> entryHandlers,
                                   RowDataHandler<CanalEntry.RowData> rowDataHandler,
                                   ThreadPoolTaskExecutor threadPoolTaskExecutor) {
        super(null, entryHandlers, rowDataHandler);
        this.threadPoolTaskExecutor = threadPoolTaskExecutor;
    }

    public AsyncMessageHandlerImpl(List<CanalEntry.EntryType> subscribeTypes,
                                   List<? extends EntryHandler> entryHandlers,
                                   RowDataHandler<CanalEntry.RowData> rowDataHandler,
                                   ThreadPoolTaskExecutor threadPoolTaskExecutor) {
        super(subscribeTypes, entryHandlers, rowDataHandler);
        this.threadPoolTaskExecutor = threadPoolTaskExecutor;
    }

    @Override
    public void handleMessage(String destination, Message message) {
        threadPoolTaskExecutor.execute(() -> super.handleMessage(destination, message));
    }

}
