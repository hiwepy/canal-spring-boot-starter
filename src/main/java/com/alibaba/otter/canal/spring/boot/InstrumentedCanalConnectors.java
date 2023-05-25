package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.spring.boot.event.MessageEvent;
import com.lmax.disruptor.dsl.Disruptor;

public final class InstrumentedCanalConnectors {

    public static CanalConnector create(CanalConnector connector, Disruptor<MessageEvent> disruptor) {
        return new InstrumentedCanalConnector(connector, disruptor);
    }

}
