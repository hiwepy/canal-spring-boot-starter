package com.alibaba.otter.canal.handler;

/**
 * @author yang peng
 * @date 2019/3/2622:06
 */
@FunctionalInterface
public interface MessageHandler<T> {

    /**
     * 处理消息
     * @param t 消息
     */
    void handleMessage(T t);

}
