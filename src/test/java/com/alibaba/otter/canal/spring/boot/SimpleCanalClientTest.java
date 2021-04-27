package com.alibaba.otter.canal.spring.boot;


import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;

/**
 * From https://github.com/alibaba/canal/tree/master/example
 * 单机模式的测试例子
 */
public class SimpleCanalClientTest {
	protected final static Logger             logger             = LoggerFactory.getLogger(SimpleCanalClientTest.class);
    
    public static void main(String args[]) {
        // 根据ip，直接创建链接，无HA的功能
        String destination = "example";
        String ip = AddressUtils.getHostIp();
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip, 11111),
            destination,
            "canal",
            "canal");

        final CanalClient clientTest = new CanalClient(connector);
        clientTest.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("## stop the canal client");
                clientTest.stop();
            } catch (Throwable e) {
                logger.warn("##something goes wrong when stopping canal:", e);
            } finally {
                logger.info("## canal client is down.");
            }
        }));
    }

}