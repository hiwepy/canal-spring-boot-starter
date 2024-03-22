package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;

/**
 * Simple 模式 Canal 客户端
 */
public class SimpleCanalClient extends AbstractCanalClient<SimpleCanalConnector> {

    private SimpleCanalClient(SimpleCanalConnector connector) {
        super(connector);
    }

    public static final class Builder extends AbstractClientBuilder<SimpleCanalClient, SimpleCanalConnector> {

        @Override
        public SimpleCanalClient build(SimpleCanalConnector connector) {
            SimpleCanalClient canalClient = new SimpleCanalClient(connector);
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
