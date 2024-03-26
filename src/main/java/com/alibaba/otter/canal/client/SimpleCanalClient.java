package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;

import java.util.List;

/**
 * Simple 模式 Canal 客户端
 */
public class SimpleCanalClient extends AbstractCanalClient<SimpleCanalConnector> {

    private SimpleCanalClient(List<SimpleCanalConnector> connectors) {
        super(connectors);
    }

    public static final class Builder extends AbstractClientBuilder<SimpleCanalClient, SimpleCanalConnector> {

        @Override
        public SimpleCanalClient build(List<SimpleCanalConnector> connectors) {
            SimpleCanalClient canalClient = new SimpleCanalClient(connectors);
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
