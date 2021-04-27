package com.alibaba.otter.canal.spring.boot.rocketmq;

/**
 * From https://github.com/alibaba/canal/tree/master/example
 */
public abstract class AbstractRocektMQTest {

    public static String  topic              = "example";
    public static String  groupId            = "group";
    public static String  nameServers        = "127.0.0.1:9876";
    public static String  accessKey          = "";
    public static String  secretKey          = "";
    public static boolean enableMessageTrace = false;
    public static String  accessChannel      = "local";
    public static String  namespace          = "";
}