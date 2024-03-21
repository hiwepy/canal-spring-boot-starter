package com.alibaba.otter.canal.spring.boot;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(CanalSimpleProperties.PREFIX)
@Data
public class CanalSimpleProperties {

    public static final int DEFAULT_PORT = 5672;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_MAX_SLEEP_MS = Integer.MAX_VALUE;
    public static final String PREFIX = "canal.simple";

    /**
     * Canal Server 主机地址。如果设置了address属性，则忽略。
      */
    private String host;
    /**
     * Canal Server 端口。如果设置了address属性，则忽略。默认为 11111
     */
    private Integer port = DEFAULT_PORT;
    /**
     * Canal Server 地址。如果设置了该属性，则忽略host和port属性。
     */
    private String addresses;
    /**
     * Canal Zookeeper 地址
     */
    private String zkServers;
    /**
     * Canal Zookeeper 地址
     */
    private String destination;
    /**
     * Canal Server 账号
     */
    private String username;
    /**
     * Canal Server 密码
     */
    private String password;
    /**
     * Socket 连接超时时间，单位：毫秒。默认为 60000
     */
    private int soTimeout     = 60000;
    /**
     * Socket 空闲超时时间，单位：毫秒。默认为 3600000
     */
    private int idleTimeout   = 60 * 60 * 1000;
    /**
     * 重试次数;设置-1时可以subscribe阻塞等待时优雅停机
     */
    private int retryTimes    = 3;
    /**
     * 重试的时间间隔，默认5秒
     */
    private int retryInterval = 5000;

}
