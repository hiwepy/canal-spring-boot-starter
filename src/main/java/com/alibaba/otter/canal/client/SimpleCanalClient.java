package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Canal client for the <strong>simple</strong> mode that consumes binlog
 * entries from one or more {@link SimpleCanalConnector} instances via direct
 * single-node TCP connections.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public class SimpleCanalClient extends AbstractCanalClient<SimpleCanalConnector> {

    private SimpleCanalClient(List<SimpleCanalConnector> connectors) {
        super(connectors);
    }

    /**
     * Resolves the Canal destination name by reflecting on the connector's
     * {@code clientIdentity} field, used as the logging/MDC destination.
     *
     * @param connector the simple connector to inspect
     * @return the destination name
     */
    @Override
    protected String getDestination(SimpleCanalConnector connector) {
        Field clientIdentityField = ReflectionUtils.findField(SimpleCanalConnector.class, "clientIdentity");
        ReflectionUtils.makeAccessible(clientIdentityField);
        ClientIdentity clientIdentity = (ClientIdentity) ReflectionUtils.getField(clientIdentityField, connector);
        return clientIdentity.getDestination();
    }

    /**
     * Fluent builder for {@link SimpleCanalClient}.
     */
    public static final class Builder extends AbstractClientBuilder<SimpleCanalClient, SimpleCanalConnector> {

        /**
         * Builds a {@link SimpleCanalClient} from the supplied connectors,
         * applying the filter, batch size, timeout, entry types and message
         * handler configured on this builder.
         *
         * @param connectors the simple connectors the client will consume from
         * @return the constructed simple Canal client
         */
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
