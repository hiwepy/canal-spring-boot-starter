package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.impl.ClusterCanalConnector;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Canal client for the <strong>cluster</strong> mode that consumes binlog
 * entries from one or more {@link ClusterCanalConnector} instances backed by a
 * Canal HA cluster (optionally via ZooKeeper failover).
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public class ClusterCanalClient extends AbstractCanalClient<ClusterCanalConnector> {

    private ClusterCanalClient(List<ClusterCanalConnector> connectors) {
        super(connectors);
    }

    /**
     * Resolves the Canal destination name by reflecting on the connector's
     * {@code destination} field, used as the logging/MDC destination.
     *
     * @param connector the cluster connector to inspect
     * @return the destination name
     */
    @Override
    /** @return return the destination. */
    protected String getDestination(ClusterCanalConnector connector) {
        Field destinationField =  ReflectionUtils.findField(ClusterCanalConnector.class, "destination");
        ReflectionUtils.makeAccessible(destinationField);
        return (String) ReflectionUtils.getField(destinationField, connector);
    }

    /**
     * Fluent builder for {@link ClusterCanalClient}.
     */
    public static final class Builder extends AbstractClientBuilder<ClusterCanalClient, ClusterCanalConnector> {

        /**
         * Builds a {@link ClusterCanalClient} from the supplied connectors,
         * applying the filter, batch size, timeout, entry types and message
         * handler configured on this builder.
         *
         * @param connectors the cluster connectors the client will consume from
         * @return the constructed cluster Canal client
         */
        @Override
        /**
         * <p>Build.</p>
         * @param connectors
         * @return the result
         */
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
