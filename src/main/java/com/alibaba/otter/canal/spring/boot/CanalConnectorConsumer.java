package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.alibaba.otter.canal.spring.boot.event.MessageEvent;
import com.alibaba.otter.canal.spring.boot.event.translator.MessageEventTwoArgTranslator;
import com.alibaba.otter.canal.spring.boot.event.translator.MessageListEventTwoArgTranslator;
import com.alibaba.otter.canal.spring.boot.utils.CanalUtils;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CanalConnectorConsumer implements CanalConnector {

    protected EventTranslatorTwoArg<MessageEvent, Boolean , Message> messageEventTranslator = new MessageEventTwoArgTranslator();
    protected EventTranslatorTwoArg<MessageEvent, Boolean, List<Message>> messageListEventTranslator = new MessageListEventTwoArgTranslator();

    protected Thread.UncaughtExceptionHandler handler            = (t, e) -> log.error("parse events has an error", e);
    protected Thread                          thread             = null;
    protected volatile boolean                running            = false;
    protected CanalConnector delegate;
    protected Disruptor<MessageEvent> disruptor;

    /**
     * 消费线程每次拉取消息的数量，默认 10
     */
    protected int batchSize = 10;
    protected Long timeout = 0L;
    protected TimeUnit unit = TimeUnit.SECONDS;
    protected boolean withoutAck;
    protected boolean requiredTimeout;

    public CanalConnectorConsumer(CanalConnector connector, Disruptor<MessageEvent> disruptor){
        Assert.notNull(connector, "connector is null");
        this.delegate = connector;
        this.disruptor = disruptor;
    }

    public CanalConnectorConsumer(CanalConnector connector, Disruptor<MessageEvent> disruptor, int batchSize){
        this(connector, disruptor);
        this.batchSize = batchSize;
    }

    public CanalConnectorConsumer(CanalConnector connector, Disruptor<MessageEvent> disruptor, int batchSize, Long timeout, TimeUnit unit){
        this(connector, disruptor);
        this.batchSize = batchSize;
        this.timeout = timeout;
        this.unit = unit;
        this.requiredTimeout = Objects.nonNull(timeout) && Objects.nonNull(unit);
    }

    public CanalConnectorConsumer(CanalConnector connector, Disruptor<MessageEvent> disruptor, int batchSize, Long timeout, TimeUnit unit, boolean withoutAck){
        this(connector, disruptor);
        this.batchSize = batchSize;
        this.timeout = timeout;
        this.unit = unit;
        this.withoutAck = withoutAck;
        this.requiredTimeout = Objects.nonNull(timeout) && Objects.nonNull(unit);
    }

    @Override
    public void connect() throws CanalClientException {
        this.delegate.connect();
        this.start();
    }

    protected void start() {
        Assert.notNull(this.delegate, "connector is null");
        this.thread = new Thread(this::process);
        this.thread.setUncaughtExceptionHandler(handler);
        this.running = true;
        this.thread.start();
    }

    protected void process() {
        int batchSize = 5 * 1024;
        while (running) {
            try {
                delegate.connect();
                delegate.subscribe();
                while (running) {
                    
                    if(this.delegate instanceof CanalMQConnector){
                        CanalMQConnector mqConnector = (CanalMQConnector) delegate;
                        List<Message> messages = withoutAck ? mqConnector.getListWithoutAck(timeout, unit) : mqConnector.getList(timeout, unit);
                        disruptor.publishEvent(messageListEventTranslator, withoutAck, messages);
                        for (Message message : messages) {
                            long batchId = message.getId();
                            int size = message.getEntries().size();
                            if (batchId == -1 || size == 0) {
                                // try {
                                // Thread.sleep(1000);
                                // } catch (InterruptedException e) {
                                // }
                            } else {
                                CanalUtils.printSummary(message, batchId, size);
                                CanalUtils.printEntry(message.getEntries());
                                // logger.info(message.toString());
                            }
                            if (batchId != -1) {
                                delegate.ack(batchId); // 提交确认
                            }
                        }
                    } else {
                        Message message;
                        if(requiredTimeout){
                            message = withoutAck ? delegate.getWithoutAck(batchSize, timeout, unit) : delegate.get(batchSize, timeout, unit);
                        } else {
                            message = withoutAck ? delegate.getWithoutAck(batchSize) : delegate.get(batchSize);
                        }
                        disruptor.publishEvent(messageEventTranslator, withoutAck, message);
                        long batchId = message.getId();
                        int size = message.getEntries().size();
                        if (batchId == -1 || size == 0) {
                            // try {
                            // Thread.sleep(1000);
                            // } catch (InterruptedException e) {
                            // }
                        } else {
                            CanalUtils.printSummary(message, batchId, size);
                            CanalUtils.printEntry(message.getEntries());
                        }
                        if (batchId != -1) {
                            delegate.ack(batchId); // 提交确认
                        }
                    }
                }
            } catch (Throwable e) {
                log.error("process error!", e);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e1) {
                    // ignore
                }
                delegate.rollback(); // 处理失败, 回滚数据
            } finally {
                delegate.disconnect();
                MDC.remove("destination");
            }
        }
    }

    @Override
    public void disconnect() throws CanalClientException {
        this.stop();
        this.delegate.disconnect();
    }

    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    @Override
    public boolean checkValid() throws CanalClientException {
        return this.delegate.checkValid();
    }

    @Override
    public void subscribe(String filter) throws CanalClientException {
        this.delegate.subscribe(filter);
    }

    @Override
    public void subscribe() throws CanalClientException {
        this.delegate.subscribe();
    }

    @Override
    public void unsubscribe() throws CanalClientException {
        this.delegate.unsubscribe();
    }

    @Override
    public Message get(int batchSize) throws CanalClientException {
        return this.delegate.get(batchSize);
    }

    @Override
    public Message get(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        return this.delegate.get(batchSize, timeout, unit);
    }

    @Override
    public Message getWithoutAck(int batchSize) throws CanalClientException {
        return this.delegate.getWithoutAck(batchSize);
    }

    @Override
    public Message getWithoutAck(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        return this.delegate.getWithoutAck(batchSize, timeout, unit);
    }

    @Override
    public void ack(long batchId) throws CanalClientException {
        this.delegate.ack(batchId);
    }

    @Override
    public void rollback(long batchId) throws CanalClientException {
        this.delegate.rollback(batchId);
    }

    @Override
    public void rollback() throws CanalClientException {
        this.delegate.rollback();
    }

}
