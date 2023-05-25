package com.alibaba.otter.canal.spring.boot.disruptor;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.spring.boot.disruptor.event.MessageEvent;
import com.lmax.disruptor.dsl.Disruptor;

import java.util.concurrent.TimeUnit;

public final class CanalDisruptorConsumers {

    public static CanalDisruptorConsumer create(CanalConnector connector, Disruptor<MessageEvent> disruptor) {
        return new CanalDisruptorConsumer(connector, disruptor);
    }

    public static CanalDisruptorConsumer create(CanalConnector connector, Disruptor<MessageEvent> disruptor, int batchSize) {
        return new CanalDisruptorConsumer(connector, disruptor, batchSize);
    }

    public static CanalDisruptorConsumer create(CanalConnector connector, Disruptor<MessageEvent> disruptor, int batchSize, Long timeout, TimeUnit unit) {
        return new CanalDisruptorConsumer(connector, disruptor, batchSize, timeout, unit);
    }

    public static CanalDisruptorConsumer create(CanalConnector connector, Disruptor<MessageEvent> disruptor, int batchSize, Long timeout, TimeUnit unit, boolean withoutAck) {
        return new CanalDisruptorConsumer(connector, disruptor, batchSize, timeout, unit, withoutAck);
    }

}
