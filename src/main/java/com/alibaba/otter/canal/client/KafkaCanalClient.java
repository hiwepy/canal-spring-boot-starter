package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.impl.ClusterCanalConnector;
import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import com.alibaba.otter.canal.handler.MessageHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.util.concurrent.TimeUnit;

/**
 * Kafka 模式 Canal 客户端
 */
@Slf4j
public class KafkaCanalClient extends AbstractMQCanalClient<KafkaCanalConnector> {

    private KafkaCanalClient(KafkaCanalConnector connector) {
        super(connector);
    }

    public static final class Builder extends AbstractClientBuilder<KafkaCanalClient, KafkaCanalConnector> {

        @Override
        public KafkaCanalClient build(KafkaCanalConnector connector) {
            KafkaCanalClient canalClient = new KafkaCanalClient(connector);
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
