package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.handler.MessageHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.util.CanalUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Canal Client 抽象类
 * @param <C> CanalConnector 实现类
 */
@Slf4j
public abstract class AbstractCanalClient<C extends CanalConnector> implements CanalClient {

    /**
     * 是否运行中
     */
    protected volatile boolean running;
    /**
     * Canal 连接器
     */
    private C connector;
    /**
     * 消息过滤
     */
    protected String filter = StringUtils.EMPTY;
    /**
     * 批处理大小
     */
    protected Integer batchSize = 1;
    /**
     * 获取数据超时时间
     */
    protected Long timeout = 1L;
    /**
     * 获取数据超时时间单位
     */
    protected TimeUnit unit = TimeUnit.SECONDS;
    /**
     * 指定订阅的事件类型，主要用于标识事务的开始，变更数据，结束
     */
    protected List<CanalEntry.EntryType> subscribeTypes = Arrays.asList(CanalEntry.EntryType.ROWDATA);
    /**
     * 消息处理器
     */
    private MessageHandler messageHandler;
    /**
     * 工作线程
     */
    private Thread workThread;

    public AbstractCanalClient(C connector) {
        this.connector = connector;
    }

    @Override
    public void start() {
        log.info("start canal client");
        workThread = new Thread(this::process);
        workThread.setName("canal-client-thread");
        running = true;
        workThread.start();
    }

    @Override
    public void stop() {
        log.info("stop canal client");
        running = false;
        if (null != workThread) {
            workThread.interrupt();
        }
    }

    @Override
    public void process() {
        while (running) {
            try {
                connector.connect();
                connector.subscribe(filter);
                while (running) {
                    Message message = connector.getWithoutAck(batchSize, timeout, unit);
                    log.info("获取消息 {}", message);
                    long batchId = message.getId();
                    if (message.getId() != -1 && message.getEntries().size() != 0) {
                        CanalUtils.printSummary(message, batchId, message.getEntries().size());
                        CanalUtils.printEntry(message.getEntries());
                        messageHandler.handleMessage(message);
                    }
                    connector.ack(batchId);
                }
            } catch (Exception e) {
                log.error("canal client 异常", e);
            } finally {
                connector.disconnect();
            }
        }
    }

    @Override
    public void destroy() throws Exception {
        stop();
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public void setMessageHandler(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public void setUnit(TimeUnit unit) {
        this.unit = unit;
    }

    public void setSubscribeTypes(List<CanalEntry.EntryType> subscribeTypes) {
        this.subscribeTypes = subscribeTypes;
    }

    public C getConnector() {
        return connector;
    }

    public MessageHandler getMessageHandler() {
        return messageHandler;
    }

}
