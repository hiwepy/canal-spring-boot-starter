package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.spring.boot.event.MessageEvent;
import com.lmax.disruptor.dsl.Disruptor;

import java.util.concurrent.TimeUnit;

public final class CanalConnectorConsumers {

    public static CanalConnectorConsumer create(CanalConnector connector, Disruptor<MessageEvent> disruptor) {
        return new CanalConnectorConsumer(connector, disruptor);
    }

    public static CanalConnectorConsumer create(CanalConnector connector, Disruptor<MessageEvent> disruptor, int batchSize) {
        return new CanalConnectorConsumer(connector, disruptor, batchSize);
    }

    public static CanalConnectorConsumer create(CanalConnector connector, Disruptor<MessageEvent> disruptor, int batchSize, Long timeout, TimeUnit unit) {
        return new CanalConnectorConsumer(connector, disruptor, batchSize, timeout, unit);
    }

    public static CanalConnectorConsumer create(CanalConnector connector, Disruptor<MessageEvent> disruptor, int batchSize, Long timeout, TimeUnit unit, boolean withoutAck) {
        return new CanalConnectorConsumer(connector, disruptor, batchSize, timeout, unit, withoutAck);
    }

}
