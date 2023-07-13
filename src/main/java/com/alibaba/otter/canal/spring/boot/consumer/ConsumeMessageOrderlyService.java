package com.alibaba.otter.canal.spring.boot.consumer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.consumer.listener.MessageListenerOrderly;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

public class ConsumeMessageOrderlyService implements ConsumeMessageService  {

    private final CanalConnectorConsumer connectorConsumer;
    private final MessageListenerOrderly messageListener;

    public ConsumeMessageOrderlyService(CanalConnectorConsumer connectorConsumer, MessageListenerOrderly messageListener) {
        this.connectorConsumer = connectorConsumer;
        this.messageListener = messageListener;
    }


    @Override
    public void start() {

    }

    @Override
    public void shutdown(long awaitTerminateMillis) {

    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {

    }

    @Override
    public int getCorePoolSize() {
        return 0;
    }

    @Override
    public void submitConsumeRequest(CanalConnector connector, boolean requireAck, List messages) {

    }

    @Override
    public void submitConsumeRequest(CanalConnector connector, boolean requireAck, List<Message> messages) {
        try {
            messageListener.consumeMessage(messages);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void submitFlatConsumeRequest(CanalConnector connector, boolean requireAck, List<FlatMessage> messages) {

    }
}
