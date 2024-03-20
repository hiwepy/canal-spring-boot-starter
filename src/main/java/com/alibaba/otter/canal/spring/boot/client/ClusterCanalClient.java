package com.alibaba.otter.canal.spring.boot.client;

import com.alibaba.otter.canal.client.impl.ClusterCanalConnector;

/**
 * 集群模式 Canal 客户端
 */
public class ClusterCanalClient extends AbstractCanalClient<ClusterCanalConnector> {

    public ClusterCanalClient(ClusterCanalConnector connector) {
        super(connector);
    }

}
