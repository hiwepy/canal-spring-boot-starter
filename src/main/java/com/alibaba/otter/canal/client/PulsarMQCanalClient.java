package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.pulsarmq.PulsarMQCanalConnector;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Canal client for the <strong>Pulsar</strong> mode that consumes flattened
 * Canal binlog messages from Pulsar topics via {@link PulsarMQCanalConnector}
 * instances.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@Slf4j
public class PulsarMQCanalClient extends AbstractMQCanalClient<PulsarMQCanalConnector> {

    private PulsarMQCanalClient(List<PulsarMQCanalConnector> connectors) {
        super(connectors);
    }

    /**
     * Resolves the destination name (Pulsar topic) by reflecting on the
     * connector's {@code topic} field, used as the logging/MDC destination.
     *
     * @param connector the Pulsar connector to inspect
     * @return the Pulsar topic name
     */
    @Override
    protected String getDestination(PulsarMQCanalConnector connector) {
        Field topicField =  ReflectionUtils.findField(PulsarMQCanalConnector.class, "topic");
        ReflectionUtils.makeAccessible(topicField);
        return (String) ReflectionUtils.getField(topicField, connector);
    }

    /**
     * Fluent builder for {@link PulsarMQCanalClient}.
     */
    public static final class Builder extends AbstractClientBuilder<PulsarMQCanalClient, PulsarMQCanalConnector> {

        /**
         * Builds a {@link PulsarMQCanalClient} from the supplied connectors,
         * applying the filter, batch size, timeout, entry types and message
         * handler configured on this builder.
         *
         * @param connectors the Pulsar connectors the client will consume from
         * @return the constructed Pulsar Canal client
         */
        @Override
        public PulsarMQCanalClient build(List<PulsarMQCanalConnector> connectors) {
            PulsarMQCanalClient canalClient = new PulsarMQCanalClient(connectors);
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
