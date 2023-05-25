package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.client.CanalConnector;

/**
 * From https://github.com/alibaba/canal/tree/master/example
 * 测试基类
 * 
 * @author jianghang 2013-4-15 下午04:17:12
 * @version 1.0.4
 */
public class AbstractCanalClientTest extends CanalClient {

    protected String destination;
    public AbstractCanalClientTest(String destination){
        this(destination, null);
    }

    public AbstractCanalClientTest(String destination, CanalConnector connector){
    	super(connector);
        this.destination = destination;
    }


}