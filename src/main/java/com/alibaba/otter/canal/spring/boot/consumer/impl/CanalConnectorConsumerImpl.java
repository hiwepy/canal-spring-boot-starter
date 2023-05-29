package com.alibaba.otter.canal.spring.boot.consumer.impl;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConnectorConsumer;
import com.alibaba.otter.canal.spring.boot.consumer.CanalConsumeMessageService;
import com.alibaba.otter.canal.spring.boot.utils.CanalUtils;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

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

            Message message;
            if(Objects.nonNull(this.getReadTimeout()) ){
                message = this.isRequireAck() ? connector.getWithoutAck(this.getBatchSize(), this.getReadTimeout(), TimeUnit.SECONDS) : connector.get(this.getBatchSize(), this.getReadTimeout(), TimeUnit.SECONDS);
            } else {
                message = this.isRequireAck() ? connector.getWithoutAck(this.getBatchSize()) : connector.get(this.getBatchSize());
            }

            getConsumeMessageService().submitConsumeRequest(connector, Arrays.asList(message));

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

    public CanalConsumeMessageService getConsumeMessageService() {
        return consumeMessageService;
    }

}
