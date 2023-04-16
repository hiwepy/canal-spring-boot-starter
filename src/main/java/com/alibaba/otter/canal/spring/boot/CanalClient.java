/*
 * Copyright (c) 2018, hiwepy (https://github.com/hiwepy).
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.alibaba.otter.canal.spring.boot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.utils.CanalUtils;

/**
 * Copy From https://github.com/alibaba/canal/tree/master/example
 */
public class CanalClient {

	protected final static Logger             logger             = LoggerFactory.getLogger(CanalClient.class);
    protected volatile boolean                running            = false;
    protected Thread.UncaughtExceptionHandler handler            = (t, e) -> logger.error("parse events has an error",
                                                                     e);
    protected Thread                          thread             = null;
    protected CanalConnector                  connector;
    
    public CanalClient(CanalConnector connector) {
    	this.connector = connector;
    }

    protected void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(this::process);
        thread.setUncaughtExceptionHandler(handler);
        running = true;
        thread.start();
    }

    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }

    }
    
    protected void process() {
        int batchSize = 5 * 1024;
        while (running) {
            try {
                connector.connect();
                connector.subscribe();
                while (running) {
                	
                	connector.get(batchSize);
                	connector.get(batchSize, null, null);
                	
                	connector.getWithoutAck(batchSize);
                	connector.getWithoutAck(batchSize, null, null);
                	
                    Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                    
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        // try {
                        // Thread.sleep(1000);
                        // } catch (InterruptedException e) {
                        // }
                    } else {
                    	CanalUtils.printSummary(message, batchId, size);
                    	CanalUtils.printEntry(message.getEntries());
                    }

                    if (batchId != -1) {
                        connector.ack(batchId); // 提交确认
                    }
                }
            } catch (Throwable e) {
                logger.error("process error!", e);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e1) {
                    // ignore
                }

                connector.rollback(); // 处理失败, 回滚数据
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }
    
}
