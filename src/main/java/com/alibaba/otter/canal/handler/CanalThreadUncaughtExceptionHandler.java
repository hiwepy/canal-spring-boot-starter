package com.alibaba.otter.canal.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class CanalThreadUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(CanalThreadUncaughtExceptionHandler.class);

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
