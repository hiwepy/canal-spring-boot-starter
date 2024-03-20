package com.alibaba.otter.canal.spring.boot.client;

import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.client.pulsarmq.PulsarMQCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import lombok.extern.slf4j.Slf4j;
import top.javatool.canal.client.handler.MessageHandler;

import java.util.List;

@Slf4j
public abstract class AbstractMQCanalClient<C extends CanalMQConnector> extends AbstractCanalClient<C> {

    public AbstractMQCanalClient(C connector) {
        super(connector);
    }

    @Override
    public void process() {
        CanalMQConnector connector = super.getConnector();
        MessageHandler messageHandler = getMessageHandler();
        while (running) {
            try {
                connector.connect();
                connector.subscribe();
                while (running) {
                    try {
                        List<FlatMessage> messages = connector.getFlatListWithoutAck(timeout, unit);
                        log.info("获取消息 {}", messages);
                        if (messages != null) {
                            for (FlatMessage flatMessage : messages) {
                                messageHandler.handleMessage(flatMessage);
                            }
                        }
                        connector.ack();
                    } catch (Exception e) {
                        log.error("canal 消费异常", e);
                    }
                }
            } catch (Exception e) {
                log.error("canal 连接异常", e);
            }
        }
        connector.unsubscribe();
        connector.disconnect();
    }

}
