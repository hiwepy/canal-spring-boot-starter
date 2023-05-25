package com.alibaba.otter.canal.spring.boot.event.factory;

import com.alibaba.otter.canal.spring.boot.event.MessageEvent;
import com.lmax.disruptor.EventFactory;

public class CanalEventFactory implements EventFactory<MessageEvent> {

   public static final CanalEventFactory INSTANCE = new CanalEventFactory();

    @Override
    public MessageEvent newInstance() {
        return new MessageEvent();
    }

}