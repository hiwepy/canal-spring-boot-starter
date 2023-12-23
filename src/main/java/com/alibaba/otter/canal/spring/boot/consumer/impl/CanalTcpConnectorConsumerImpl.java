package com.alibaba.otter.canal.spring.boot.consumer.impl;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConnectorConsumer;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConsumeMessageService;
import com.alibaba.otter.canal.spring.boot.exception.CanalConsumeException;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Canal TCP Connector Consumer
 */
@Slf4j
public class CanalTcpConnectorConsumerImpl extends CanalConnectorConsumer<CanalConnector> {

    private final CanalConsumeMessageService consumeMessageService;

    public CanalTcpConnectorConsumerImpl(List<CanalConnector> connectors, CanalConsumeMessageService consumeMessageService){
        super(connectors);
        this.consumeMessageService = consumeMessageService;
    }

    @Override
    public void consumeMessage(CanalConnector connector) {
        // this consumer support this Simple connector
        if(!SimpleCanalConnector.class.isAssignableFrom(connector.getClass())){
            throw new CanalConsumeException("consumer not support this connector");
        }
        // get messages
        Message message;
        if(this.isRequireAck()){
            // get message without Ack
            message = Objects.nonNull(this.getReadTimeout())  ?  connector.getWithoutAck(this.getBatchSize(), this.getReadTimeout(), TimeUnit.SECONDS) :
                    connector.getWithoutAck(this.getBatchSize());
        } else {
            // get message with auto Ack
            message = Objects.nonNull(this.getReadTimeout()) ? connector.get(this.getBatchSize(), this.getReadTimeout(), TimeUnit.SECONDS) :
                    connector.get(this.getBatchSize());
        }
        // submit consume request
        getConsumeMessageService().submitConsumeRequest(connector, this.isRequireAck(), Arrays.asList(message));
    }

    public CanalConsumeMessageService getConsumeMessageService() {
        return consumeMessageService;
    }

}
