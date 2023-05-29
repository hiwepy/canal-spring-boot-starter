package com.alibaba.otter.canal.spring.boot.consumer.impl;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConnectorConsumer;
import com.alibaba.otter.canal.spring.boot.disruptor.event.MessageEvent;
import com.alibaba.otter.canal.spring.boot.disruptor.event.translator.MessageEventTwoArgTranslator;
import com.alibaba.otter.canal.spring.boot.utils.CanalUtils;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CanalConnectorDisruptorConsumerImpl extends CanalConnectorConsumer<CanalConnector> {

    protected EventTranslatorTwoArg<MessageEvent, Boolean , Message> messageEventTranslator = new MessageEventTwoArgTranslator();
    protected final Disruptor<MessageEvent> disruptor;

    public CanalConnectorDisruptorConsumerImpl(List<CanalConnector> connectors, Disruptor<MessageEvent> disruptor){
        super(connectors);
        this.disruptor = disruptor;
    }

    @Override
    public void consumeMessage(CanalConnector connector) {
        try {

            connector.connect();
            connector.subscribe();

            Message message;
            if(Objects.nonNull(this.getReadTimeout()) ){
                message = this.isRequireAck() ? connector.getWithoutAck(this.getBatchSize(), this.getReadTimeout(), TimeUnit.SECONDS) : connector.get(this.getBatchSize(), this.getReadTimeout(), TimeUnit.SECONDS);
            } else {
                message = this.isRequireAck() ? connector.getWithoutAck(this.getBatchSize()) : connector.get(this.getBatchSize());
            }

            disruptor.publishEvent(messageEventTranslator, this.isRequireAck(), message);

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
                connector.ack(batchId); // 提交确认
            }

        } catch (Throwable e) {
            log.error("process error!", e);
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e1) {
                // ignore
            }
            connector.rollback(); // 处理失败, 回滚数据
        } finally {
            connector.disconnect();
            MDC.remove("destination");
        }
    }

}
