package com.alibaba.otter.canal.spring.boot.consumer;

import com.alibaba.otter.canal.client.CanalConnector;

import java.util.List;

public interface ConsumeMessageService<T> {

    default void start() {};

    default void shutdown(long awaitTerminateMillis) {}

    default void updateCorePoolSize(int corePoolSize){}

    default int getCorePoolSize() { return 0;}

    void submitConsumeRequest( CanalConnector connector, boolean requireAck, List<T> messages);

}
