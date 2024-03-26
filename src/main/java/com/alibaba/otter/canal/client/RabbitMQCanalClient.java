package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.rabbitmq.RabbitMQCanalConnector;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * RabbitMQ 模式 Canal 客户端
 */
public class RabbitMQCanalClient extends AbstractMQCanalClient<RabbitMQCanalConnector> {

    private RabbitMQCanalClient(List<RabbitMQCanalConnector> connectors) {
        super(connectors);
    }

    @Override
    protected String getDestination(RabbitMQCanalConnector connector) {
        Field nameServerField =  ReflectionUtils.findField(RabbitMQCanalConnector.class, "nameServer");
        ReflectionUtils.makeAccessible(nameServerField);
        return (String) ReflectionUtils.getField(nameServerField, connector);
    }

    public static final class Builder extends AbstractClientBuilder<RabbitMQCanalClient, RabbitMQCanalConnector> {

        @Override
        public RabbitMQCanalClient build(List<RabbitMQCanalConnector> connectors) {
            RabbitMQCanalClient canalClient = new RabbitMQCanalClient(connectors);
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
