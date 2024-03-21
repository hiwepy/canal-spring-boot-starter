package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import com.alibaba.otter.canal.handler.MessageHandler;
import org.apache.commons.lang.StringUtils;

import java.util.concurrent.TimeUnit;

/**
 * Simple 模式 Canal 客户端
 */
public class SimpleCanalClient extends AbstractCanalClient<SimpleCanalConnector> {

    private SimpleCanalClient(SimpleCanalConnector connector) {
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

        public SimpleCanalClient build(SimpleCanalConnector connector) {
            SimpleCanalClient simpleCanalClient = new SimpleCanalClient(connector);
            simpleCanalClient.setBatchSize(batchSize);
            simpleCanalClient.setFilter(filter);
            simpleCanalClient.setMessageHandler(messageHandler);
            simpleCanalClient.setTimeout(timeout);
            simpleCanalClient.setUnit(unit);
            return simpleCanalClient;
        }
    }


}
