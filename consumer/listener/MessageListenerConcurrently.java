package com.alibaba.otter.canal.spring.boot.consumer.listener;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;

import java.util.List;

/**
 * A MessageListenerConcurrently object is used to receive asynchronously delivered messages concurrently
 */
public interface MessageListenerConcurrently extends MessageListener {

    /**
     * It is not recommend to throw exception,rather than returning ConsumeConcurrentlyStatus.RECONSUME_LATER if
     * consumption failure
     *
     * @param messages messages.size() >= 1<br> CanalConnectorConsumer.consumeMessageBatchMaxSize=1,you can modify here
     * @return The consume status
     */
    default ConsumeConcurrentlyStatus consumeMessage(List<Message> messages) throws Exception {
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    /**
     * It is not recommend to throw exception,rather than returning ConsumeConcurrentlyStatus.RECONSUME_LATER if
     * consumption failure
     *
     * @param messages messages.size() >= 1<br> CanalConnectorConsumer.consumeMessageBatchMaxSize=1,you can modify here
     * @return The consume status
     */
    default ConsumeConcurrentlyStatus consumeFlatMessage(List<FlatMessage> messages) throws Exception {
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

}