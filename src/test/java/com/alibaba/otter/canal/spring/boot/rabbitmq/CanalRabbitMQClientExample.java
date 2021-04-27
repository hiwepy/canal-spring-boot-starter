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
package com.alibaba.otter.canal.spring.boot.rabbitmq;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.client.rabbitmq.RabbitMQCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.rocketmq.AbstractRocektMQTest;
import com.alibaba.otter.canal.spring.boot.utils.CanalUtils;

/**
 * Rabbitmq client example
 */
public class CanalRabbitMQClientExample extends AbstractRocektMQTest {

    protected final static Logger           logger  = LoggerFactory.getLogger(CanalRabbitMQClientExample.class);

    private RabbitMQCanalConnector          connector;

    private static volatile boolean         running = false;

    private Thread                          thread  = null;

    private Thread.UncaughtExceptionHandler handler = (t, e) -> logger.error("parse events has an error", e);

    public CanalRabbitMQClientExample(String nameServer, String vhost, String queueName, String accessKey, String secretKey,
            String username, String password, Long resourceOwnerId, boolean flatMessage) {
        connector = new RabbitMQCanalConnector(nameServer, vhost, queueName, accessKey, secretKey, username, password, resourceOwnerId, flatMessage);
    }

    public static void main(String[] args) {
        try {
            final CanalRabbitMQClientExample rocketMQClientExample = new CanalRabbitMQClientExample(nameServers,
                topic,
                groupId,
                enableMessageTrace,
                accessKey,
                secretKey,
                accessChannel,
                namespace);
            logger.info("## Start the rocketmq consumer: {}-{}", topic, groupId);
            rocketMQClientExample.start();
            logger.info("## The canal rocketmq consumer is running now ......");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    logger.info("## Stop the rocketmq consumer");
                    rocketMQClientExample.stop();
                } catch (Throwable e) {
                    logger.warn("## Something goes wrong when stopping rocketmq consumer:", e);
                } finally {
                    logger.info("## Rocketmq consumer is down.");
                }
            }));
            while (running)
                ;
        } catch (Throwable e) {
            logger.error("## Something going wrong when starting up the rocketmq consumer:", e);
            System.exit(0);
        }
    }

    public void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(this::process);
        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }

    public void stop() {
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

    private void process() {
        while (!running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        while (running) {
            try {
                connector.connect();
                connector.subscribe();
                while (running) {
                	connector.ack();
                	 
                	List<FlatMessage> messages1 = connector.getFlatListWithoutAck(1000L, TimeUnit.MILLISECONDS);
                	
                    List<Message> messages = connector.getListWithoutAck(1000L, TimeUnit.MILLISECONDS); // 获取message
                    for (Message message : messages) {
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
                            // logger.info(message.toString());
                        }
                    }

                    connector.ack(); // 提交确认
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        connector.unsubscribe();
        // connector.stopRunning();
    }
}