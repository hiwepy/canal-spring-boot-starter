package com.alibaba.otter.canal.spring.boot.client;

import com.alibaba.otter.canal.client.pulsarmq.PulsarMQCanalConnector;
import lombok.extern.slf4j.Slf4j;

/**
 * PulsarMQ 模式 Canal 客户端
 */
@Slf4j
public class PulsarMQCanalClient extends AbstractMQCanalClient<PulsarMQCanalConnector> {

    public PulsarMQCanalClient(PulsarMQCanalConnector connector) {
        super(connector);
    }

}
