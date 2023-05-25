package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.alibaba.otter.canal.spring.boot.event.FlatMessageEvent;
import com.alibaba.otter.canal.spring.boot.event.MessageEvent;
import com.alibaba.otter.canal.spring.boot.event.translator.FlatMessageEventOneArgTranslator;
import com.alibaba.otter.canal.spring.boot.event.translator.MessageEventOneArgTranslator;
import com.alibaba.otter.canal.spring.boot.utils.CanalUtils;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.util.Assert;

import java.util.concurrent.TimeUnit;

@Slf4j
public class InstrumentedCanalConnector implements CanalConnector {

    protected EventTranslatorOneArg<MessageEvent, Message> messageEventTranslator = new MessageEventOneArgTranslator();
    protected EventTranslatorOneArg<FlatMessageEvent, FlatMessage> flatMessageEventTranslator = new FlatMessageEventOneArgTranslator();

    protected Thread.UncaughtExceptionHandler handler            = (t, e) -> log.error("parse events has an error", e);
    protected Thread                          thread             = null;
    protected volatile boolean                running            = false;
    protected CanalConnector delegate;
    protected Disruptor<MessageEvent> disruptor;

    public InstrumentedCanalConnector(CanalConnector connector, Disruptor<MessageEvent> disruptor){
        Assert.notNull(connector, "connector is null");
        this.delegate = connector;
        this.disruptor = disruptor;
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

                    }
                    delegate.get(batchSize);
                    delegate.get(batchSize, null, null);

                    delegate.getWithoutAck(batchSize);
                    delegate.getWithoutAck(batchSize, null, null);

                    Message message = delegate.getWithoutAck(batchSize); // 获取指定数量的数据

                    disruptor.publishEvent(messageEventTranslator, message);

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
