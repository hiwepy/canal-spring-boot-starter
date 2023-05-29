package com.alibaba.otter.canal.spring.boot.consumer.impl;

import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConnectorConsumer;
import com.alibaba.otter.canal.spring.boot.disruptor.event.MessageEvent;
import com.alibaba.otter.canal.spring.boot.disruptor.event.translator.MessageListEventTwoArgTranslator;
import com.alibaba.otter.canal.spring.boot.utils.CanalUtils;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CanalMQDisruptorConnectorConsumerImpl extends CanalConnectorConsumer<CanalMQConnector> {

    protected EventTranslatorTwoArg<MessageEvent, Boolean, List<Message>> messageListEventTranslator = new MessageListEventTwoArgTranslator();
    protected final Disruptor<MessageEvent> disruptor;

    public CanalMQDisruptorConnectorConsumerImpl(List<CanalMQConnector> connectors, Disruptor<MessageEvent> disruptor){
        super(connectors);
        this.disruptor = disruptor;
    }

    @Override
    public void consumeMessage(CanalMQConnector connector) {
        try {

            connector.connect();
            connector.subscribe();
            List<Message> messages = this.isRequireAck() ? connector.getListWithoutAck(this.getReadTimeout(), TimeUnit.SECONDS) : connector.getList(this.getReadTimeout(), TimeUnit.SECONDS);
            disruptor.publishEvent(messageListEventTranslator, this.isRequireAck(), messages);

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
                    connector.ack(batchId); // 提交确认
                }
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
