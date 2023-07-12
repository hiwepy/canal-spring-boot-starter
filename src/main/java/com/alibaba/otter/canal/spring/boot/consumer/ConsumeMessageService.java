package com.alibaba.otter.canal.spring.boot.consumer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;

import java.util.List;

public interface ConsumeMessageService {

    void start();

    void shutdown(long awaitTerminateMillis);

    void updateCorePoolSize(int corePoolSize);

    int getCorePoolSize();

    void submitConsumeRequest( CanalConnector connector, boolean requireAck, List<Message> messages);

    void submitFlatConsumeRequest( CanalConnector connector, boolean requireAck, List<FlatMessage> messages);

}
