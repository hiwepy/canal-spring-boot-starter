package com.alibaba.otter.canal.spring.boot.client;

import org.springframework.beans.factory.DisposableBean;

/**
 * Canal Client 接口
 */
public interface CanalClient extends DisposableBean {

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
    void process();

}