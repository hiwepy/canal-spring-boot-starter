package com.alibaba.otter.canal.handler.impl;


import com.alibaba.otter.canal.handler.AbstractFlatMessageHandler;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.handler.RowDataHandler;
import com.alibaba.otter.canal.protocol.FlatMessage;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class AsyncFlatMessageHandlerImpl extends AbstractFlatMessageHandler {

    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    public AsyncFlatMessageHandlerImpl(List<? extends EntryHandler> entryHandlers, RowDataHandler<List<Map<String, String>>> rowDataHandler, ThreadPoolTaskExecutor threadPoolTaskExecutor) {
        super(entryHandlers, rowDataHandler);
        this.threadPoolTaskExecutor = threadPoolTaskExecutor;
    }

    @Override
    public void handleMessage(FlatMessage flatMessage) {
        threadPoolTaskExecutor.execute(() -> super.handleMessage(flatMessage));
    }
}
