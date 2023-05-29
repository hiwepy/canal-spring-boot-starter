package com.alibaba.otter.canal.spring.boot.consumer;

import com.alibaba.otter.canal.protocol.Message;

import java.util.List;

public interface CanalConsumeMessageService {

    void start();

    void shutdown(long awaitTerminateMillis);

    void updateCorePoolSize(int corePoolSize);

    void incCorePoolSize();

    void decCorePoolSize();

    int getCorePoolSize();

    void submitConsumeRequest(
            final List<Message> msgs,
            final boolean dispathToConsume);

}
