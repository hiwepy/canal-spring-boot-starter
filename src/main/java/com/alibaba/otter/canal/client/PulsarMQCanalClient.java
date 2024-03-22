package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.pulsarmq.PulsarMQCanalConnector;
import lombok.extern.slf4j.Slf4j;

/**
 * PulsarMQ 模式 Canal 客户端
 */
@Slf4j
public class PulsarMQCanalClient extends AbstractMQCanalClient<PulsarMQCanalConnector> {

    private PulsarMQCanalClient(PulsarMQCanalConnector connector) {
        super(connector);
    }

    public static final class Builder extends AbstractClientBuilder<PulsarMQCanalClient, PulsarMQCanalConnector> {

        @Override
        public PulsarMQCanalClient build(PulsarMQCanalConnector connector) {
            PulsarMQCanalClient canalClient = new PulsarMQCanalClient(connector);
            canalClient.setBatchSize(batchSize);
            canalClient.setFilter(filter);
            canalClient.setMessageHandler(messageHandler);
            canalClient.setTimeout(timeout);
            canalClient.setUnit(unit);
            canalClient.setSubscribeTypes(subscribeTypes);
            return canalClient;
        }

    }
}
