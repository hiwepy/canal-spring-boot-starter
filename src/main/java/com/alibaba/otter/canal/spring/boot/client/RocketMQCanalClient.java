package com.alibaba.otter.canal.spring.boot.client;

import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;

/**
 * RocketMQ 模式 Canal 客户端
 */
public class RocketMQCanalClient extends AbstractMQCanalClient<RocketMQCanalConnector> {

    public RocketMQCanalClient(RocketMQCanalConnector connector) {
        super(connector);
    }

}
