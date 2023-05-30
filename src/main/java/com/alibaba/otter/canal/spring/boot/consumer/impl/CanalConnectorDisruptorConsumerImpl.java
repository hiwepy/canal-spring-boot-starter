package com.alibaba.otter.canal.spring.boot.consumer.impl;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConnectorConsumer;
import com.alibaba.otter.canal.spring.boot.disruptor.event.MessageEvent;
import com.alibaba.otter.canal.spring.boot.disruptor.event.translator.MessageEventTranslator;
import com.alibaba.otter.canal.spring.boot.exception.CanalConsumeException;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CanalConnectorDisruptorConsumerImpl extends CanalConnectorConsumer<CanalConnector> {

    protected EventTranslatorOneArg<MessageEvent, Message> messageEventTranslator = new MessageEventTranslator();
    protected Disruptor<MessageEvent> disruptor;

    public CanalConnectorDisruptorConsumerImpl(List<CanalConnector> connectors, Disruptor<MessageEvent> disruptor){
        super(connectors);
        this.disruptor = disruptor;
    }

    @Override
    public void consumeMessage(CanalConnector connector) {
        // this consumer not support this mq connector
        if(CanalMQConnector.class.isAssignableFrom(connector.getClass())){
            throw new CanalConsumeException("consumer not support this connector");
        }
        // get message with auto Ack
        Message message = Objects.nonNull(this.getReadTimeout()) ? connector.get(this.getBatchSize(), this.getReadTimeout(), TimeUnit.SECONDS) :
                connector.get(this.getBatchSize());
        // dispatch message event
        disruptor.publishEvent(messageEventTranslator, message);
    }

}
