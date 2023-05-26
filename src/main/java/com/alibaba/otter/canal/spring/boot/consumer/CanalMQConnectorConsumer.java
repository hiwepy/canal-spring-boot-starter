package com.alibaba.otter.canal.spring.boot.consumer;

import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.message.FlatMessageListener;
import com.alibaba.otter.canal.spring.boot.message.MessageListener;
import com.alibaba.otter.canal.spring.boot.utils.CanalUtils;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.util.Assert;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CanalMQConnectorConsumer {

    protected Thread.UncaughtExceptionHandler handler            = (t, e) -> log.error("parse events has an error", e);
    protected Thread                          thread             = null;
    protected volatile boolean                running            = false;
    protected CanalMQConnector connector;
    /**
     * 消费线程每次拉取消息的数量，默认 10
     */
    protected int batchSize = 10;
    protected Long timeout = 0L;
    protected TimeUnit unit = TimeUnit.SECONDS;
    protected boolean withoutAck;
    protected boolean requiredTimeout;
    protected List<MessageListener> messageListeners;
    protected List<FlatMessageListener> flatMessageListeners;

    public CanalMQConnectorConsumer(CanalMQConnector connector){
        Assert.notNull(connector, "connector is null");
        this.connector = connector;
    }

    public void start() {
        Assert.notNull(this.connector, "connector is null");
        this.thread = new Thread(this::process);
        this.thread.setUncaughtExceptionHandler(handler);
        this.running = true;
        this.thread.start();
    }

    protected void process() {
        int batchSize = 5 * 1024;
        while (running) {
            try {
                connector.connect();
                connector.subscribe();
                while (running) {

                    if(this.connector instanceof CanalMQConnector){
                        CanalMQConnector mqConnector = (CanalMQConnector) connector;
                        List<Message> messages = withoutAck ? mqConnector.getListWithoutAck(timeout, unit) : mqConnector.getList(timeout, unit);
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
                            if (batchId != -1) {
                                connector.ack(batchId); // 提交确认
                            }
                        }
                    } else {
                        Message message;
                        if(requiredTimeout){
                            message = withoutAck ? connector.getWithoutAck(batchSize, timeout, unit) : connector.get(batchSize, timeout, unit);
                        } else {
                            message = withoutAck ? connector.getWithoutAck(batchSize) : connector.get(batchSize);
                        }
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
                }
            } catch (Throwable e) {
                log.error("process error!", e);
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

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public void setUnit(TimeUnit unit) {
        this.unit = unit;
    }

    public void setWithoutAck(boolean withoutAck) {
        this.withoutAck = withoutAck;
    }

    public void setMessageListeners(List<MessageListener> messageListeners) {
        this.messageListeners = messageListeners;
    }

    public void setFlatMessageListeners(List<FlatMessageListener> flatMessageListeners) {
        this.flatMessageListeners = flatMessageListeners;
    }
}
