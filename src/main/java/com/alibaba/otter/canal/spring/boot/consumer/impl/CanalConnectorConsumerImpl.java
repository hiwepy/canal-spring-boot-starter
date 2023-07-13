package com.alibaba.otter.canal.spring.boot.consumer.impl;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConnectorConsumer;
import com.alibaba.otter.canal.spring.boot.exception.CanalConsumeException;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * CanalConnector Consumer
 */
@Slf4j
public class CanalConnectorConsumerImpl extends CanalConnectorConsumer<CanalConnector, Message> {

    public CanalConnectorConsumerImpl(List<CanalConnector> connectors){
        super(connectors);
    }

    @Override
    public void consumeMessage(CanalConnector connector) {
        // this consumer not support this mq connector
        if(CanalMQConnector.class.isAssignableFrom(connector.getClass())){
            throw new CanalConsumeException("consumer not support this connector");
        }
        // get read timeout
        Long readTimeout = Objects.nonNull(this.getReadTimeout())  ? this.getReadTimeout() : 0L;
        // get messages
        Message message;
        if(this.isRequireAck()){
            // get message without Ack
            message = connector.getWithoutAck(this.getBatchSize(), readTimeout, TimeUnit.SECONDS);
        } else {
            // get message with auto Ack
            message = connector.get(this.getBatchSize(), readTimeout, TimeUnit.SECONDS);
        }
        // submit consume request
        getConsumeMessageService().submitConsumeRequest(connector, this.isRequireAck(), Arrays.asList(message));

    }

}
