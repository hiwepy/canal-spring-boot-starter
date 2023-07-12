package com.alibaba.otter.canal.spring.boot.consumer.listener;


import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;

import java.util.List;

/**
 * A MessageListenerOrderly object is used to receive messages orderly. One queue by one thread
 */
public interface MessageListenerOrderly extends MessageListener {
    /**
     * It is not recommend to throw exception,rather than returning ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT
     * if consumption failure
     *
     * @param messages msgs.size() >= 1<br> CanalConnectorConsumer.consumeMessageBatchMaxSize=1,you can modify here
     * @return The consume status
     */
    default ConsumeOrderlyStatus consumeMessages(List<Message> messages) throws Exception {
        return ConsumeOrderlyStatus.SUCCESS;
    }

    /**
     * It is not recommend to throw exception,rather than returning ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT
     * if consumption failure
     *
     * @param messages msgs.size() >= 1<br> CanalConnectorConsumer.consumeMessageBatchMaxSize=1,you can modify here
     * @return The consume status
     */
    default ConsumeOrderlyStatus consumeFlatMessages(List<FlatMessage> messages) throws Exception {
        return ConsumeOrderlyStatus.SUCCESS;
    }
}
