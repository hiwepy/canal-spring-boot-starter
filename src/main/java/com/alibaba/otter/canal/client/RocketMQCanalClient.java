package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;

/**
 * RocketMQ 模式 Canal 客户端
 */
public class RocketMQCanalClient extends AbstractMQCanalClient<RocketMQCanalConnector> {

    public RocketMQCanalClient(RocketMQCanalConnector connector) {
        super(connector);
    }

    public static final class Builder extends AbstractClientBuilder<RocketMQCanalClient, RocketMQCanalConnector> {

        @Override
        public RocketMQCanalClient build(RocketMQCanalConnector connector) {
            RocketMQCanalClient canalClient = new RocketMQCanalClient(connector);
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
