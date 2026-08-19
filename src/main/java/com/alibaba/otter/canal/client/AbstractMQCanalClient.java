package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.handler.MessageHandler;
import com.alibaba.otter.canal.protocol.FlatMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * Base implementation of a Canal client that consumes flattened Canal messages
 * ({@link FlatMessage}) from a message-queue-backed {@link CanalMQConnector}
 * (e.g. Kafka, RocketMQ, Pulsar, RabbitMQ).
 * <p>
 * Each worker thread connects, subscribes, polls flat messages without ack,
 * dispatches them to the configured {@link MessageHandler}, and acks the batch
 * once all messages have been handled. Consumption and connection errors are
 * logged and the loop resumes; on shutdown the connector is unsubscribed and
 * disconnected.
 * </p>
 *
 * @param <C> the {@link CanalMQConnector} implementation type
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public abstract class AbstractMQCanalClient<C extends CanalMQConnector> extends AbstractCanalClient<C> {

    private static final Logger log = LoggerFactory.getLogger(AbstractMQCanalClient.class);

    /**
     * @param connectors the MQ-backed connectors this client will consume from
     */
    public AbstractMQCanalClient(List<C> connectors) {
        super(connectors);
    }

    @Override
    /**
     * <p>Process.</p>
     * @param connector
     */
    public void process(C connector) {
        String destination = this.getDestination(connector);
        MessageHandler messageHandler = super.getMessageHandler();
        while (running) {
            try {
                connector.connect();
                connector.subscribe();
                while (running) {
                    try {
                        List<FlatMessage> messages = connector.getFlatListWithoutAck(timeout, unit);
                        if (CollectionUtils.isEmpty(messages)) {
                            continue;
                        }
                        for (FlatMessage flatMessage : messages) {
                            messageHandler.handleMessage(destination, flatMessage);
                        }
                        connector.ack();
                    } catch (Exception e) {
                        log.error("canal consume error", e);
                    }
                }
            } catch (Exception e) {
                log.error("canal connection error", e);
            }
        }
        connector.unsubscribe();
        connector.disconnect();
    }

}
