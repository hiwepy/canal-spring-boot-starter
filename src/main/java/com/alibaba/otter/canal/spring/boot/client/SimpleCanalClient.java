package com.alibaba.otter.canal.spring.boot.client;

import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;

/**
 * Simple 模式 Canal 客户端
 */
public class SimpleCanalClient extends AbstractCanalClient<SimpleCanalConnector> {

    public SimpleCanalClient(SimpleCanalConnector connector) {
        super(connector);
    }

}
