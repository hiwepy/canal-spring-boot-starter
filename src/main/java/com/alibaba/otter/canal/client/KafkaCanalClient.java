package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Kafka 模式 Canal 客户端
 */
@Slf4j
public class KafkaCanalClient extends AbstractMQCanalClient<KafkaCanalConnector> {

    private KafkaCanalClient(List<KafkaCanalConnector> connectors) {
        super(connectors);
    }

    @Override
    protected String getDestination(KafkaCanalConnector connector) {
        Field topicField =  ReflectionUtils.findField(KafkaCanalConnector.class, "topic");
        ReflectionUtils.makeAccessible(topicField);
        return (String) ReflectionUtils.getField(topicField, connector);
    }

    public static final class Builder extends AbstractClientBuilder<KafkaCanalClient, KafkaCanalConnector> {

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
