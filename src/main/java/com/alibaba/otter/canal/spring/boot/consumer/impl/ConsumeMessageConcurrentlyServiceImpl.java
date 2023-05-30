package com.alibaba.otter.canal.spring.boot.consumer.impl;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.CanalConsumerProperties;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConsumeMessageService;
import com.alibaba.otter.canal.spring.boot.consumer.ThreadFactoryImpl;
import com.alibaba.otter.canal.spring.boot.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.otter.canal.spring.boot.consumer.listener.ConsumeReturnType;
import com.alibaba.otter.canal.spring.boot.consumer.listener.MessageListenerConcurrently;
import com.alibaba.otter.canal.spring.boot.utils.ThreadUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public class ConsumeMessageConcurrentlyServiceImpl implements CanalConsumeMessageService {

    private final CanalConsumerProperties consumerProperties;
    private final MessageListenerConcurrently messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;
    private final ScheduledExecutorService scheduledExecutorService;

    public ConsumeMessageConcurrentlyServiceImpl(CanalConsumerProperties consumerProperties, MessageListenerConcurrently messageListener) {

        this.consumerProperties = consumerProperties;
        this.messageListener = messageListener;
        this.consumeRequestQueue = new LinkedBlockingQueue<>();
        this.consumeExecutor = new ThreadPoolExecutor(
                this.consumerProperties.getConsumeThreadMin(),
                this.consumerProperties.getConsumeThreadMax(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.consumeRequestQueue,
                new ThreadFactoryImpl("Canal_ConsumeMessageThread_"));
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("Canal_ConsumeMessageScheduledThread_"));
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown(long awaitTerminateMillis) {
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
                && corePoolSize <= Short.MAX_VALUE
                && corePoolSize < this.consumerProperties.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public void submitConsumeRequest(CanalConnector connector, List<Message> messages) {
        // get message consume batch size
        int consumeBatchSize = this.consumerProperties.getConsumeMessageBatchMaxSize();
        if (messages.size() <= consumeBatchSize) {
            ConsumeRequest consumeRequest = new ConsumeRequest(connector, messages);
            try {
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                this.submitConsumeRequestLater(consumeRequest);
            }
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
                ConsumeRequest consumeRequest = new ConsumeRequest(connector, msgThis);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    for (; total < messages.size(); total++) {
                        msgThis.add(messages.get(total));
                    }

                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }

    private void submitConsumeRequestLater(ConsumeRequest consumeRequest) {
        this.scheduledExecutorService.schedule(() -> {
           consumeExecutor.submit(consumeRequest);
        }, 5000, TimeUnit.MILLISECONDS);
    }

    class ConsumeRequest implements Runnable {

        private final CanalConnector connector;
        private final List<Message> messages;

        public ConsumeRequest(CanalConnector connector, List<Message> messages) {
            this.connector = connector;
            this.messages = messages;
        }

        public List<Message> getMessages() {
            return messages;
        }

        @Override
        public void run() {

            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyServiceImpl.this.messageListener;
            ConsumeConcurrentlyStatus status = null;

            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                status = listener.consumeMessage(Collections.unmodifiableList(this.getMessages()));
            } catch (Throwable e) {
                log.warn("consumeMessage exception: {} Msgs: {}", e.getLocalizedMessage(), this.getMessages(), e);
                hasException = true;
            }
            long consumeRT = System.currentTimeMillis() - beginTimestamp;
            if (null == status) {
                if (hasException) {
                    returnType = ConsumeReturnType.EXCEPTION;
                } else {
                    returnType = ConsumeReturnType.RETURNNULL;
                }
            } else if (consumeRT >= consumerProperties.getConsumeTimeout() * 60 * 1000) {
                returnType = ConsumeReturnType.TIME_OUT;
            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                returnType = ConsumeReturnType.FAILED;
            } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                returnType = ConsumeReturnType.SUCCESS;
            }
            if (null == status) {
                log.warn("consumeMessage return null, Msgs: {} ",  this.getMessages() );
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            ConsumeMessageConcurrentlyServiceImpl.this.processConsumeResult(connector, status, this);

        }

    }

    public void processConsumeResult(CanalConnector connector, ConsumeConcurrentlyStatus status, ConsumeRequest consumeRequest ) {

        if (consumeRequest.getMessages().isEmpty()) {
            return;
        }

        // 循环所有消息
        for (Message message: consumeRequest.getMessages()) {
            connector.ack(message.getId());
        }

        switch (status) {
            case CONSUME_SUCCESS:

                break;
            case RECONSUME_LATER:
                break;
            default:
                break;
        }

       /* List<Message> msgBackFailed = new ArrayList<>(consumeRequest.getMessages().size());
        for (int i = ackIndex + 1; i < consumeRequest.getMessages().size(); i++) {
            Message msg = consumeRequest.getMsgs().get(i);
            boolean result = this.sendMessageBack(msg, context);
            if (!result) {
                msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                msgBackFailed.add(msg);
            }
        }

        if (!msgBackFailed.isEmpty()) {
            consumeRequest.getMessages().removeAll(msgBackFailed);

            this.submitConsumeRequestLater(msgBackFailed);
        }*/

    }

}
