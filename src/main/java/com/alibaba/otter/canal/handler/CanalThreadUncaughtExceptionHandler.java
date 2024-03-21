package com.alibaba.otter.canal.handler;

import lombok.extern.slf4j.Slf4j;

/**
 * @author yang peng
 * @date 2019/4/117:29
 */
@Slf4j
public class CanalThreadUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        log.error("thread "+ t.getName()+" have a exception",e);
    }

}
