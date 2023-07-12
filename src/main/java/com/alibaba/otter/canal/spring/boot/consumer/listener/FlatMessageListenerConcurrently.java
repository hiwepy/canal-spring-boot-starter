package com.alibaba.otter.canal.spring.boot.consumer.listener;

import com.alibaba.otter.canal.protocol.FlatMessage;

import java.util.List;

/**
 * A MessageListenerConcurrently object is used to receive asynchronously delivered messages concurrently
 */
public interface FlatMessageListenerConcurrently extends MessageListener {

    /**
     * It is not recommend to throw exception,rather than returning ConsumeConcurrentlyStatus.RECONSUME_LATER if
     * consumption failure
     *
     * @param messages messages.size() >= 1<br> DefaultConsumer.consumeMessageBatchMaxSize=1,you can modify here
     * @return The consume status
     */
    ConsumeConcurrentlyStatus consumeMessage(List<FlatMessage> messages) throws Exception;

}