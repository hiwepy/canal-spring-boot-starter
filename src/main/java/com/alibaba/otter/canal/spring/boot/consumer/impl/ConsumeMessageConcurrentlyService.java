package com.alibaba.otter.canal.spring.boot.consumer.impl;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConnectorConsumer;
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
public class ConsumeMessageConcurrentlyService  implements CanalConsumeMessageService {

    protected CanalConnector connector;
    private final CanalConnectorConsumer defaultConsumer;
    private final MessageListenerConcurrently messageListener;
    private final BlockingQueue<Runnable> consumeRequestQueue;
    private final ThreadPoolExecutor consumeExecutor;
    private final ScheduledExecutorService scheduledExecutorService;

    public ConsumeMessageConcurrentlyService(CanalConnectorConsumerImpl defaultConsumer, MessageListenerConcurrently messageListener) {

        this.defaultConsumer = defaultConsumer;
        this.messageListener = messageListener;
        this.consumeRequestQueue = new LinkedBlockingQueue<>();
        this.consumeExecutor = new ThreadPoolExecutor(
                this.defaultConsumer.getConsumeThreadMin(),
                this.defaultConsumer.getConsumeThreadMax(),
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
                && corePoolSize < this.defaultConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {

    }

    @Override
    public void decCorePoolSize() {

    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public void submitConsumeRequest(List<Message> msgs, boolean dispathToConsume) {
        // get message consume batch size
        int consumeBatchSize = this.defaultConsumer.getConsumeMessageBatchMaxSize();
        if (msgs.size() <= consumeBatchSize) {
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs);
            try {
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                this.submitConsumeRequestLater(consumeRequest);
            }
        } else {
            for (int total = 0; total < msgs.size(); ) {
                List<Message> msgThis = new ArrayList<>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }
                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
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

        private final List<Message> msgs;

        public ConsumeRequest(List<Message> msgs) {
            this.msgs = msgs;
        }

        public List<Message> getMsgs() {
            return msgs;
        }

        @Override
        public void run() {

            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;
            ConsumeConcurrentlyStatus status = null;

            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                status = listener.consumeMessage(Collections.unmodifiableList(this.getMsgs()));
            } catch (Throwable e) {
                log.warn("consumeMessage exception: {} Msgs: {}", e.getLocalizedMessage(), this.getMsgs(), e);
                hasException = true;
            }
            long consumeRT = System.currentTimeMillis() - beginTimestamp;
            if (null == status) {
                if (hasException) {
                    returnType = ConsumeReturnType.EXCEPTION;
                } else {
                    returnType = ConsumeReturnType.RETURNNULL;
                }
            } else if (consumeRT >= defaultConsumer.getConsumeTimeout() * 60 * 1000) {
                returnType = ConsumeReturnType.TIME_OUT;
            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                returnType = ConsumeReturnType.FAILED;
            } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                returnType = ConsumeReturnType.SUCCESS;
            }
            if (null == status) {
                log.warn("consumeMessage return null, Msgs: {} ",  msgs );
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }


        }

    }

}
