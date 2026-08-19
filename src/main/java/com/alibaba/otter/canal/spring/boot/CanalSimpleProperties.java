package com.alibaba.otter.canal.spring.boot;

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
/**
 * <p>Auto-configuration for CanalSimpleProperties.</p>
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
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

    public List<CanalSimpleProperties.Instance> getInstances() { return instances; }
    /** @param instances set the instances. */
    public void setInstances(List<CanalSimpleProperties.Instance> instances) { this.instances = instances; }

    /**
     * Connection definition for a single Canal server instance (simple mode).
     */
    public static class Instance {

        /** Canal server host address. */
        private String host;
        /** Canal server TCP port. */
        private Integer port = DEFAULT_PORT;
        /** Canal destination (instance name) to subscribe to. */
        private String destination;
        /** Canal server account username, if authentication is enabled. */
        private String username;
        /** Canal server account password, if authentication is enabled. */
        private String password;
        /** Socket connect timeout in milliseconds. */
        private int soTimeout = 60000;
        /** Socket idle timeout in milliseconds. */
        private int idleTimeout = 60 * 60 * 1000;
        /** Number of retries on connection failure. */
        private int retryTimes = 3;
        /** Interval between retries in milliseconds. */
        private int retryInterval = 5000;

        /** @return return the host. */
        public String getHost() { return host; }
        /** @param host set the host. */
        public void setHost(String host) { this.host = host; }
        /** @return return the port. */
        public Integer getPort() { return port; }
        /** @param port set the port. */
        public void setPort(Integer port) { this.port = port; }
        /** @return return the destination. */
        public String getDestination() { return destination; }
        /** @param destination set the destination. */
        public void setDestination(String destination) { this.destination = destination; }
        /** @return return the username. */
        public String getUsername() { return username; }
        /** @param username set the username. */
        public void setUsername(String username) { this.username = username; }
        /** @return return the password. */
        public String getPassword() { return password; }
        /** @param password set the password. */
        public void setPassword(String password) { this.password = password; }
        /** @return return the so timeout. */
        public int getSoTimeout() { return soTimeout; }
        /** @param soTimeout set the so timeout. */
        public void setSoTimeout(int soTimeout) { this.soTimeout = soTimeout; }
        /** @return return the idle timeout. */
        public int getIdleTimeout() { return idleTimeout; }
        /** @param idleTimeout set the idle timeout. */
        public void setIdleTimeout(int idleTimeout) { this.idleTimeout = idleTimeout; }
        /** @return return the retry times. */
        public int getRetryTimes() { return retryTimes; }
        /** @param retryTimes set the retry times. */
        public void setRetryTimes(int retryTimes) { this.retryTimes = retryTimes; }
        /** @return return the retry interval. */
        public int getRetryInterval() { return retryInterval; }
        /** @param retryInterval set the retry interval. */
        public void setRetryInterval(int retryInterval) { this.retryInterval = retryInterval; }

    }

}
