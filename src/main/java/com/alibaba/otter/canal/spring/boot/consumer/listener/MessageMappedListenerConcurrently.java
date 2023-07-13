package com.alibaba.otter.canal.spring.boot.consumer.listener;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;

import java.util.List;

public class MessageMappedListenerConcurrently implements MessageListenerConcurrently{

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<Message> messages) throws Exception {
        return null;
    }

    @Override
    public ConsumeConcurrentlyStatus consumeFlatMessage(List<FlatMessage> messages) throws Exception {
        return null;
    }

}
