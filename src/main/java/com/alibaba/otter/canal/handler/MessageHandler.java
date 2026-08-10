package com.alibaba.otter.canal.handler;

/**
 * Functional contract for handling a Canal message of the given type.
 * <p>
 * Implementations receive the Canal destination name together with the polled
 * message (either a {@code Message} for direct clients or a {@code FlatMessage}
 * for MQ-based clients) and dispatch it to the appropriate row/entry handlers.
 * </p>
 *
 * @param <T> the message type handled (e.g. {@code Message} or {@code FlatMessage})
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@FunctionalInterface
public interface MessageHandler<T> {

    /**
     * Handles a single Canal message.
     *
     * @param destination the Canal destination (instance name or topic) the message originated from
     * @param t           the message to handle
     */
    void handleMessage(String destination, T t);

}
