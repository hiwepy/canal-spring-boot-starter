package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.pulsarmq.PulsarMQCanalConnector;
import com.alibaba.otter.canal.handler.MessageHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.util.concurrent.TimeUnit;

/**
 * PulsarMQ 模式 Canal 客户端
 */
@Slf4j
public class PulsarMQCanalClient extends AbstractMQCanalClient<PulsarMQCanalConnector> {

    private PulsarMQCanalClient(PulsarMQCanalConnector connector) {
        super(connector);
    }

    public static final class Builder {

        private String filter = StringUtils.EMPTY;
        private Integer batchSize = 1;
        private Long timeout = 1L;
        private TimeUnit unit = TimeUnit.SECONDS;
        private MessageHandler messageHandler;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder filter(String filter) {
            this.filter = filter;
            return this;
        }

        public Builder batchSize(Integer batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder timeout(Long timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder unit(TimeUnit unit) {
            this.unit = unit;
            return this;
        }

        public Builder messageHandler(MessageHandler messageHandler) {
            this.messageHandler = messageHandler;
            return this;
        }

        public PulsarMQCanalClient build(PulsarMQCanalConnector connector) {
            PulsarMQCanalClient canalClient = new PulsarMQCanalClient(connector);
            canalClient.setBatchSize(batchSize);
            canalClient.setFilter(filter);
            canalClient.setMessageHandler(messageHandler);
            canalClient.setTimeout(timeout);
            canalClient.setUnit(unit);
            return canalClient;
        }

    }
}
