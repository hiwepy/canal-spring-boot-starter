package com.alibaba.otter.canal.client.disruptor.factory;

import com.alibaba.otter.canal.client.disruptor.MessageEvent;
import com.alibaba.otter.canal.protocol.Message;
import com.lmax.disruptor.EventFactory;

import java.util.List;

public class CanalMessageFactory implements EventFactory<List<Message>> {

    public static final CanalMessageFactory INSTANCE = new CanalMessageFactory();

    @Override
    public List<Message> newInstance() {
        return new MessageEvent();
    }

}
