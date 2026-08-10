package com.alibaba.otter.canal.spring.boot;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Connection properties for the Canal <strong>simple</strong> client mode.
 * <p>
 * Bound to the {@code canal.simple.*} configuration namespace. Holds a list of
 * {@link Instance} definitions, each describing a direct single-node TCP
 * connection to a Canal server. Multiple instances can be configured to fan out
 * consumption across several Canal destinations.
 * </p>
 *
 * <h3>Configuration keys</h3>
 * <ul>
 *   <li>{@code canal.simple.instances} — list of Canal server connection definitions</li>
 * </ul>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@ConfigurationProperties(CanalSimpleProperties.PREFIX)
@Getter
@Setter
@ToString
public class CanalSimpleProperties {

    /** Default Canal server TCP port. */
    public static final int DEFAULT_PORT = 11111;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_MAX_SLEEP_MS = Integer.MAX_VALUE;
    /** Configuration prefix used by Spring Boot to bind properties. */
    public static final String PREFIX = "canal.simple";

    /**
     * List of Canal server instance connection definitions. Each entry produces
     * one {@code SimpleCanalConnector} when the simple client is built.
     */
    private List<CanalSimpleProperties.Instance> instances = new ArrayList<>();

    /**
     * Connection definition for a single Canal server instance (simple mode).
     */
    @Data
    public static class Instance {

        /** Canal server host address. */
        private String host;
        /** Canal server TCP port. Defaults to {@value #DEFAULT_PORT}. */
        private Integer port = DEFAULT_PORT;
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
