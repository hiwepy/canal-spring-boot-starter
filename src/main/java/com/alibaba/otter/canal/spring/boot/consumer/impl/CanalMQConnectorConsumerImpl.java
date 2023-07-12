package com.alibaba.otter.canal.spring.boot.consumer.impl;

import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConnectorConsumer;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * CanalMQConnector Consumer
 */
@Slf4j
public class CanalMQConnectorConsumerImpl extends CanalConnectorConsumer<CanalMQConnector> {

    public CanalMQConnectorConsumerImpl(List<CanalMQConnector> connectors){
        super(connectors);
    }

    @Override
    public void consumeMessage(CanalMQConnector connector) {
        // get read timeout
        Long readTimeout = Objects.nonNull(this.getReadTimeout())  ? this.getReadTimeout() : 0L;
        // check if flat message
        if(this.isFlatMessage()){
            // get messages
            List<FlatMessage> flatMessages;
            if(this.isRequireAck()){
                // get message without Ack
                flatMessages = connector.getFlatListWithoutAck(readTimeout, TimeUnit.SECONDS);
            } else {
                // get message with auto Ack
                flatMessages = connector.getFlatList(readTimeout, TimeUnit.SECONDS);
            }
            // submit consume request
            getConsumeMessageService().submitFlatConsumeRequest(connector, this.isRequireAck(), flatMessages);
        } else {
            // get messages
            List<Message> messages;
            if(this.isRequireAck()){
                // get message without Ack
                messages = connector.getListWithoutAck(readTimeout, TimeUnit.SECONDS);
            } else {
                // get message with auto Ack
                messages = connector.getList(readTimeout, TimeUnit.SECONDS);
            }
            // submit consume request
            getConsumeMessageService().submitConsumeRequest(connector, this.isRequireAck(), messages);
        }
    }


}
