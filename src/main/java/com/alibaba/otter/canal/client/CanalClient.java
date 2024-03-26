package com.alibaba.otter.canal.client;

import org.springframework.beans.factory.DisposableBean;

/**
 * Canal Client 接口
 */
public interface CanalClient<C extends CanalConnector> extends DisposableBean {

    /**
     * 启动客户端
     */
    void start();

    /**
     * 停止客户端
     */
    void stop();

    /**
     * 处理
     */
    void process(C connector);

}
