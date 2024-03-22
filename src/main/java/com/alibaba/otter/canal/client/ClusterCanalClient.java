package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.impl.ClusterCanalConnector;

/**
 * 集群模式 Canal 客户端
 */
public class ClusterCanalClient extends AbstractCanalClient<ClusterCanalConnector> {

    private ClusterCanalClient(ClusterCanalConnector connector) {
        super(connector);
    }

    public static final class Builder extends AbstractClientBuilder<ClusterCanalClient, ClusterCanalConnector> {

        @Override
        public ClusterCanalClient build(ClusterCanalConnector connector) {
            ClusterCanalClient canalClient = new ClusterCanalClient(connector);
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
