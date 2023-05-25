package com.alibaba.otter.canal.spring.boot.disruptor;

import com.alibaba.otter.canal.spring.boot.event.MessageEvent;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WorkHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface CanalEventHandler extends EventHandler<MessageEvent>, WorkHandler<MessageEvent> {

    Logger log = LoggerFactory.getLogger(CanalEventHandler.class);

    @Override
    default void onEvent(MessageEvent event, long sequence, boolean endOfBatch) throws Exception {
        log.info("consumer: {} Event: uuid={},sequence={},endOfBatch={}",
                Thread.currentThread().getName(), event.getUuid(), sequence, endOfBatch);
        this.onEvent(event);
    }

    @Override
    default void onEvent(MessageEvent event) throws Exception {

    }

}
