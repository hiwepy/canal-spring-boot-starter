package com.alibaba.otter.canal.client.disruptor.factory;

import com.alibaba.otter.canal.client.disruptor.MessageEvent;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.lmax.disruptor.EventFactory;

import java.util.List;

public class CanalFlatMessageFactory implements EventFactory<List<FlatMessage>> {

    public static final CanalFlatMessageFactory INSTANCE = new CanalFlatMessageFactory();

    @Override
    public List<FlatMessage> newInstance() {
        return new MessageEvent();
    }

}
