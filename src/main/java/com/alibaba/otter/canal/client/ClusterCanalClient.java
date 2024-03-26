package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.impl.ClusterCanalConnector;

import java.util.List;

/**
 * 集群模式 Canal 客户端
 */
public class ClusterCanalClient extends AbstractCanalClient<ClusterCanalConnector> {

    private ClusterCanalClient(List<ClusterCanalConnector> connectors) {
        super(connectors);
    }

    public static final class Builder extends AbstractClientBuilder<ClusterCanalClient, ClusterCanalConnector> {

        @Override
        public ClusterCanalClient build(List<ClusterCanalConnector> connectors) {
            ClusterCanalClient canalClient = new ClusterCanalClient(connectors);
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
