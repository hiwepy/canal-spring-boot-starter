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
    public void submitConsumeRequest(CanalConnector connector, boolean requireAck, List<Message> messages) {
        // get message consume batch size
        int consumeBatchSize = this.connectorConsumer.getConsumeMessageBatchMaxSize();
        if (messages.size() <= consumeBatchSize) {
            messageListener.consumeMessages(messages);
        } else {
            for (int total = 0; total < messages.size(); ) {
                List<Message> msgThis = new ArrayList<>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < messages.size()) {
                        msgThis.add(messages.get(total));
                    } else {
                        break;
                    }
                }
                try {
                } catch (RejectedExecutionException e) {
                    // 如果队列满了，直接丢弃后面的消息
                    for (; total < messages.size(); total++) {
                        msgThis.add(messages.get(total));
                    }
                }
            }
        }
    }

    @Override
    public void submitFlatConsumeRequest(CanalConnector connector, boolean requireAck, List<FlatMessage> messages) {

    }
}
