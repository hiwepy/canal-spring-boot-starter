package com.alibaba.otter.canal.spring.boot.consumer.impl;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConnectorConsumer;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConsumeMessageService;
import com.alibaba.otter.canal.spring.boot.utils.CanalUtils;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * CanalConnector Consumer
 */
@Slf4j
public class CanalConnectorConsumerImpl extends CanalConnectorConsumer<CanalConnector> {

    private final CanalConsumeMessageService consumeMessageService;

    public CanalConnectorConsumerImpl(List<CanalConnector> connectors, CanalConsumeMessageService consumeMessageService){
        super(connectors);
        this.consumeMessageService = consumeMessageService;
    }

    @Override
    public void consumeMessage(CanalConnector connector) {
        try {

            connector.connect();
            connector.subscribe();

            if( connector instanceof CanalMQConnector){
                CanalMQConnector mqConnector = (CanalMQConnector) connector;
                List<Message> messages = withoutAck ? mqConnector.getListWithoutAck(timeout, unit) : mqConnector.getList(timeout, unit);
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
            } else {

                Message message;
                if(Objects.nonNull(timeout) && Objects.nonNull(unit)){
                    message = withoutAck ? connector.getWithoutAck(consumeMessageBatchMaxSize, timeout, unit) : connector.get(consumeMessageBatchMaxSize, timeout, unit);
                } else {
                    message = withoutAck ? connector.getWithoutAck(consumeMessageBatchMaxSize) : connector.get(consumeMessageBatchMaxSize);
                }

                getConsumeMessageService().submitConsumeRequest(Arrays.asList(message), true);

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

    public CanalConsumeMessageService getConsumeMessageService() {
        return consumeMessageService;
    }

}
