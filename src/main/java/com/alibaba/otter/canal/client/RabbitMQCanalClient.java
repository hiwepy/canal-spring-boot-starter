package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.rabbitmq.RabbitMQCanalConnector;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Canal client for the <strong>RabbitMQ</strong> mode that consumes flattened
 * Canal binlog messages from RabbitMQ queues via {@link RabbitMQCanalConnector}
 * instances.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public class RabbitMQCanalClient extends AbstractMQCanalClient<RabbitMQCanalConnector> {

    private RabbitMQCanalClient(List<RabbitMQCanalConnector> connectors) {
        super(connectors);
    }

    /**
     * Resolves the destination name (RabbitMQ broker address) by reflecting on
     * the connector's {@code nameServer} field, used as the logging/MDC
     * destination.
     *
     * @param connector the RabbitMQ connector to inspect
     * @return the resolved destination name
     */
    @Override
    /** @return return the destination. */
    protected String getDestination(RabbitMQCanalConnector connector) {
        Field nameServerField =  ReflectionUtils.findField(RabbitMQCanalConnector.class, "nameServer");
        ReflectionUtils.makeAccessible(nameServerField);
        return (String) ReflectionUtils.getField(nameServerField, connector);
    }

    /**
     * Fluent builder for {@link RabbitMQCanalClient}.
     */
    public static final class Builder extends AbstractClientBuilder<RabbitMQCanalClient, RabbitMQCanalConnector> {

        /**
         * Builds a {@link RabbitMQCanalClient} from the supplied connectors,
         * applying the filter, batch size, timeout, entry types and message
         * handler configured on this builder.
         *
         * @param connectors the RabbitMQ connectors the client will consume from
         * @return the constructed RabbitMQ Canal client
         */
        @Override
        /**
         * <p>Build.</p>
         * @param connectors
         * @return the result
         */
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
