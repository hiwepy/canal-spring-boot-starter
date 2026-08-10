package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Canal client for the <strong>RocketMQ</strong> mode that consumes flattened
 * Canal binlog messages from RocketMQ topics via {@link RocketMQCanalConnector}
 * instances.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public class RocketMQCanalClient extends AbstractMQCanalClient<RocketMQCanalConnector> {

    /**
     * @param connectors the RocketMQ connectors this client will consume from
     */
    public RocketMQCanalClient(List<RocketMQCanalConnector> connectors) {
        super(connectors);
    }

    /**
     * Resolves the destination name (RocketMQ topic) by reflecting on the
     * connector's {@code topic} field, used as the logging/MDC destination.
     *
     * @param connector the RocketMQ connector to inspect
     * @return the RocketMQ topic name
     */
    @Override
    protected String getDestination(RocketMQCanalConnector connector) {
        Field topicField =  ReflectionUtils.findField(RocketMQCanalConnector.class, "topic");
        ReflectionUtils.makeAccessible(topicField);
        return (String) ReflectionUtils.getField(topicField, connector);
    }

    /**
     * Fluent builder for {@link RocketMQCanalClient}.
     */
    public static final class Builder extends AbstractClientBuilder<RocketMQCanalClient, RocketMQCanalConnector> {

        /**
         * Builds a {@link RocketMQCanalClient} from the supplied connectors,
         * applying the filter, batch size, timeout, entry types and message
         * handler configured on this builder.
         *
         * @param connectors the RocketMQ connectors the client will consume from
         * @return the constructed RocketMQ Canal client
         */
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
