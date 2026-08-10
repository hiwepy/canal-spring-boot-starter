package com.alibaba.otter.canal.spring.boot;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Connection properties for the Canal <strong>cluster</strong> client mode.
 * <p>
 * Bound to the {@code canal.cluster.*} configuration namespace. Each
 * {@link Instance} describes a high-availability Canal cluster connection,
 * optionally backed by a ZooKeeper ensemble for failover. When
 * {@link Instance#getZkServers()} is set it takes precedence over
 * {@link Instance#getAddresses()}.
 * </p>
 *
 * <h3>Configuration keys</h3>
 * <ul>
 *   <li>{@code canal.cluster.instances} — list of Canal cluster connection definitions</li>
 * </ul>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@ConfigurationProperties(CanalClusterProperties.PREFIX)
@Getter
@Setter
@ToString
public class CanalClusterProperties {

    /** Default Canal server TCP port. */
    public static final int DEFAULT_PORT = 11111;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_MAX_SLEEP_MS = Integer.MAX_VALUE;
    /** Configuration prefix used by Spring Boot to bind properties. */
    public static final String PREFIX = "canal.cluster";

    /**
     * List of Canal cluster instance connection definitions. Each entry produces
     * one {@code ClusterCanalConnector} when the cluster client is built.
     */
    private List<CanalClusterProperties.Instance> instances = new ArrayList<>();

    /**
     * Connection definition for a single Canal cluster instance.
     */
    @Data
    public static class Instance {

        /** Comma-separated list of Canal server addresses. */
        private String addresses;
        /**
         * ZooKeeper address list used for cluster failover. When set, it takes
         * precedence over {@link #addresses}.
         */
        private String zkServers;
        /** Canal destination (instance name) to subscribe to. */
        private String destination;
        /** Canal server account username, if authentication is enabled. */
        private String username;
        /** Canal server account password, if authentication is enabled. */
        private String password;
        /** Socket connect timeout in milliseconds. Defaults to {@code 60000}. */
        private int soTimeout     = 60000;
        /** Socket idle timeout in milliseconds. Defaults to {@code 3600000} (1 hour). */
        private int idleTimeout   = 60 * 60 * 1000;
        /**
         * Number of retries on connection failure. Set to {@code -1} to allow
         * graceful shutdown while {@code subscribe} is blocking.
         */
        private int retryTimes    = 3;
        /** Interval between retries in milliseconds. Defaults to {@code 5000}. */
        private int retryInterval = 5000;

    }

}
