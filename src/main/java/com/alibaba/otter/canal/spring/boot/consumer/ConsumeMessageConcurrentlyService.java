package com.alibaba.otter.canal.spring.boot.consumer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
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
public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {

    private final CanalConnectorConsumer canalConnectorConsumer;

    private final MessageListenerConcurrently messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;
    private final ScheduledExecutorService scheduledExecutorService;

    public ConsumeMessageConcurrentlyService(CanalConnectorConsumer canalConnectorConsumer, MessageListenerConcurrently messageListener) {

        this.canalConnectorConsumer = canalConnectorConsumer;
        this.messageListener = messageListener;
        this.consumeRequestQueue = new LinkedBlockingQueue<>();
        this.consumeExecutor = new ThreadPoolExecutor(
                this.canalConnectorConsumer.getConsumeThreadMin(),
                this.canalConnectorConsumer.getConsumeThreadMax(),
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
                && corePoolSize < this.canalConnectorConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public void submitConsumeRequest(CanalConnector connector, boolean requireAck, List<Message> messages) {
        // get message consume batch size
        int consumeBatchSize = this.canalConnectorConsumer.getConsumeMessageBatchMaxSize();
        if (messages.size() <= consumeBatchSize) {
            ConsumeRequest consumeRequest = new ConsumeRequest(connector, requireAck, messages);
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
                ConsumeRequest consumeRequest = new ConsumeRequest(connector, requireAck, msgThis);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    // 如果队列满了，直接丢弃后面的消息
                    for (; total < messages.size(); total++) {
                        msgThis.add(messages.get(total));
                    }
                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }

    @Override
    public void submitFlatConsumeRequest(CanalConnector connector, boolean requireAck, List<FlatMessage> messages) {

    }

    class ConsumeRequest implements Runnable {

        private CanalConnector connector;
        /**
         * Maximum amount of time in minutes a message may block the consuming thread.
         */
        private long consumeTimeout = 15;
        private boolean requireAck;
        private MessageListenerConcurrently listener;
        private List<Message> messages;

        public ConsumeRequest(CanalConnector connector, boolean requireAck, List<Message> messages) {
            this.connector = connector;
            this.requireAck = requireAck;
            this.messages = messages;
        }

        public List<Message> getMessages() {
            return messages;
        }

        @Override
        public void run() {

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
            } else if (consumeRT >= consumeTimeout * 60 * 1000) {
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

            this.processConsumeResult(connector, status, this);

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

    private void submitConsumeRequestLater(ConsumeRequest consumeRequest) {
        this.scheduledExecutorService.schedule(() -> {
           consumeExecutor.submit(consumeRequest);
        }, 5000, TimeUnit.MILLISECONDS);
    }

}
