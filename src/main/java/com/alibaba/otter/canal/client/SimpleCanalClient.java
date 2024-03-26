package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Simple 模式 Canal 客户端
 */
public class SimpleCanalClient extends AbstractCanalClient<SimpleCanalConnector> {

    private SimpleCanalClient(List<SimpleCanalConnector> connectors) {
        super(connectors);
    }

    @Override
    protected String getDestination(SimpleCanalConnector connector) {
        Field clientIdentityField = ReflectionUtils.findField(SimpleCanalConnector.class, "clientIdentity");
        ReflectionUtils.makeAccessible(clientIdentityField);
        ClientIdentity clientIdentity = (ClientIdentity) ReflectionUtils.getField(clientIdentityField, connector);
        return clientIdentity.getDestination();
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
