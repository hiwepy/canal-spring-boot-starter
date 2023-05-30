package com.alibaba.otter.canal.spring.boot.consumer.impl;

import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConnectorConsumer;
import com.alibaba.otter.canal.spring.boot.disruptor.event.MessageEvent;
import com.alibaba.otter.canal.spring.boot.disruptor.event.translator.MessageListEventTranslator;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CanalMQDisruptorConnectorConsumerImpl extends CanalConnectorConsumer<CanalMQConnector> {

    protected EventTranslatorOneArg<MessageEvent, List<Message>> messageListEventTranslator = new MessageListEventTranslator();
    protected Disruptor<MessageEvent> disruptor;

    public CanalMQDisruptorConnectorConsumerImpl(List<CanalMQConnector> connectors, Disruptor<MessageEvent> disruptor){
        super(connectors);
        this.disruptor = disruptor;
    }

    @Override
    public void consumeMessage(CanalMQConnector connector) {
        // get message with auto Ack
        List<Message> messages = Objects.nonNull(this.getReadTimeout()) ? connector.getList(this.getReadTimeout(), TimeUnit.SECONDS)
                : connector.getList(0L, TimeUnit.SECONDS);
        // dispatch message event
        disruptor.publishEvent(messageListEventTranslator, messages);
    }

}
