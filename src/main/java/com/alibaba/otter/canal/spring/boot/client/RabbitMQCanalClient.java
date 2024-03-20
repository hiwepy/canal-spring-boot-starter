package com.alibaba.otter.canal.spring.boot.client;

import com.alibaba.otter.canal.client.rabbitmq.RabbitMQCanalConnector;

/**
 * RabbitMQ 模式 Canal 客户端
 */
public class RabbitMQCanalClient extends AbstractMQCanalClient<RabbitMQCanalConnector> {

    public RabbitMQCanalClient(RabbitMQCanalConnector connector) {
        super(connector);
    }

}
