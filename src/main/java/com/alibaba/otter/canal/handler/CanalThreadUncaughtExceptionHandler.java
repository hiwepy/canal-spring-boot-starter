package com.alibaba.otter.canal.handler;

import lombok.extern.slf4j.Slf4j;

/**
 * Logs uncaught exceptions thrown by Canal worker threads.
 * <p>
 * Installed as the {@link Thread.UncaughtExceptionHandler} on every Canal
 * client worker thread so that unexpected failures are recorded rather than
 * swallowed.
 * </p>
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */
@Slf4j
public class CanalThreadUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    /**
     * Logs the throwable raised by the given thread.
     *
     * @param t the thread that terminated with an uncaught exception
     * @param e the uncaught throwable
     */
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        log.error("thread "+ t.getName()+" have a exception",e);
    }

}
