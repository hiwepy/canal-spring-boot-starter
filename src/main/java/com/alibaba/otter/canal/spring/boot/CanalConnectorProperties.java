package com.alibaba.otter.canal.spring.boot;

import lombok.Data;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(CanalConnectorProperties.PREFIX)
@Data
public class CanalConnectorProperties {

    public static final int DEFAULT_PORT = 5672;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_MAX_SLEEP_MS = Integer.MAX_VALUE;
    public static final String PREFIX = "canal.connector";

    /**
     * Whether Enable Canal Connector.
     */
    private boolean enabled = false;

    /**
     * Canal主机地址。如果设置了address属性，则忽略。
      */
    private String host;
    /**
     * Canal端口。如果设置了address属性，则忽略。默认为 5672
     */
    private Integer port = DEFAULT_PORT;

    private String zkServers;

    /**
     * Canal地址。如果设置了该属性，则忽略host和port属性。
     */
    private String addresses;

    private String destination;
    private String username;

    private String password;

    private int soTimeout     = 60000;
    private int idleTimeout   = 60 * 60 * 1000;

}
