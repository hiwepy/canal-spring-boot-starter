package com.alibaba.otter.canal.spring.boot.handler;

public interface MessageHandler<T> {

    void handleMessage(T t);

}
