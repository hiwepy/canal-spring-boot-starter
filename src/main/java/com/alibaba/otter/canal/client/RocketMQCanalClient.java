package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * RocketMQ 模式 Canal 客户端
 */
public class RocketMQCanalClient extends AbstractMQCanalClient<RocketMQCanalConnector> {

    public RocketMQCanalClient(List<RocketMQCanalConnector> connectors) {
        super(connectors);
    }

    @Override
    protected String getDestination(RocketMQCanalConnector connector) {
        Field topicField =  ReflectionUtils.findField(RocketMQCanalConnector.class, "topic");
        ReflectionUtils.makeAccessible(topicField);
        return (String) ReflectionUtils.getField(topicField, connector);
    }

    public static final class Builder extends AbstractClientBuilder<RocketMQCanalClient, RocketMQCanalConnector> {

        @Override
        public RocketMQCanalClient build(List<RocketMQCanalConnector> connectors) {
            RocketMQCanalClient canalClient = new RocketMQCanalClient(connectors);
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
