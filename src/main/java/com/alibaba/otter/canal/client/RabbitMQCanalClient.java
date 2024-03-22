package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.rabbitmq.RabbitMQCanalConnector;

/**
 * RabbitMQ 模式 Canal 客户端
 */
public class RabbitMQCanalClient extends AbstractMQCanalClient<RabbitMQCanalConnector> {

    private RabbitMQCanalClient(RabbitMQCanalConnector connector) {
        super(connector);
    }

    public static final class Builder extends AbstractClientBuilder<RabbitMQCanalClient, RabbitMQCanalConnector> {

        @Override
        public RabbitMQCanalClient build(RabbitMQCanalConnector connector) {
            RabbitMQCanalClient canalClient = new RabbitMQCanalClient(connector);
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
