package com.alibaba.otter.canal.client;

import org.springframework.beans.factory.DisposableBean;

/**
 * Canal client contract shared by all client modes (simple, cluster, MQ-based).
 * <p>
 * Implementations are Spring-managed beans whose lifecycle is driven by the
 * {@code start}/{@code stop} methods registered through
 * {@code @Bean(initMethod = "start", destroyMethod = "stop")}. Extends
 * {@link DisposableBean} so the Spring container also triggers cleanup on
 * context close.
 * </p>
 *
 * @param <C> the {@link CanalConnector} type used by this client
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */
public interface CanalClient<C extends CanalConnector> extends DisposableBean {

    /**
     * Starts the client, spawning worker threads that consume from the configured connectors.
     */
    void start();

    /**
     * Stops the client, interrupting worker threads and releasing resources.
     */
    void stop();

    /**
     * Consumes and processes events from a single connector until the client is stopped.
     *
     * @param connector the connector to consume from
     */
    void process(C connector);

}
