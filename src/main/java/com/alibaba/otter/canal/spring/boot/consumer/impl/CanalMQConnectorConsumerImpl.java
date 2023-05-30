package com.alibaba.otter.canal.spring.boot.consumer.impl;

import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConnectorConsumer;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConsumeMessageService;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CanalMQConnectorConsumerImpl extends CanalConnectorConsumer<CanalMQConnector> {

    private final CanalConsumeMessageService consumeMessageService;

    public CanalMQConnectorConsumerImpl(List<CanalMQConnector> connectors, CanalConsumeMessageService consumeMessageService){
        super(connectors);
        this.consumeMessageService = consumeMessageService;
    }

    @Override
    public void consumeMessage(CanalMQConnector connector) {
        // get messages
        List<Message> messages;
        if(this.isRequireAck()){
            // get message without Ack
            messages = Objects.nonNull(this.getReadTimeout())  ?  connector.getListWithoutAck(this.getReadTimeout(), TimeUnit.SECONDS) :
                    connector.getListWithoutAck(this.getReadTimeout(), TimeUnit.SECONDS);
        } else {
            // get message with auto Ack
            messages = Objects.nonNull(this.getReadTimeout()) ? connector.getList(this.getReadTimeout(), TimeUnit.SECONDS) :
                    connector.getList(this.getReadTimeout(), TimeUnit.SECONDS);
        }
        // submit consume request
        getConsumeMessageService().submitConsumeRequest(connector, this.isRequireAck(), messages);
    }

    public CanalConsumeMessageService getConsumeMessageService() {
        return consumeMessageService;
    }

}
