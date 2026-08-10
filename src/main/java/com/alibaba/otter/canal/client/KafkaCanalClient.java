package com.alibaba.otter.canal.client;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ReflectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

/**
 * Canal client for the <strong>Kafka</strong> mode that consumes flattened
 * Canal binlog messages from Kafka topics via {@link KafkaCanalConnector}
 * instances.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public class KafkaCanalClient extends AbstractMQCanalClient<KafkaCanalConnector> {

    private KafkaCanalClient(List<KafkaCanalConnector> connectors) {
        super(connectors);
    }

    /**
     * Resolves the destination name (Kafka topic) by reflecting on the
     * connector's {@code topic} field, used as the logging/MDC destination.
     *
     * @param connector the Kafka connector to inspect
     * @return the Kafka topic name
     */
    @Override
    protected String getDestination(KafkaCanalConnector connector) {
        Field topicField =  ReflectionUtils.findField(KafkaCanalConnector.class, "topic");
        ReflectionUtils.makeAccessible(topicField);
        return (String) ReflectionUtils.getField(topicField, connector);
    }

    /**
     * Fluent builder for {@link KafkaCanalClient}.
     */
    public static final class Builder extends AbstractClientBuilder<KafkaCanalClient, KafkaCanalConnector> {

        /**
         * Builds a {@link KafkaCanalClient} from the supplied connectors,
         * applying the filter, batch size, timeout, entry types and message
         * handler configured on this builder.
         *
         * @param connectors the Kafka connectors the client will consume from
         * @return the constructed Kafka Canal client
         */
        @Override
        public KafkaCanalClient build(List<KafkaCanalConnector> connectors) {
            KafkaCanalClient canalClient = new KafkaCanalClient(connectors);
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
