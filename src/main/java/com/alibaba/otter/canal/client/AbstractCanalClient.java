package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.handler.MessageHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.util.CanalUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Canal Client 抽象类
 * @param <C> CanalConnector 实现类
 */
@Slf4j
public abstract class AbstractCanalClient<C extends CanalConnector> implements CanalClient<C> {

    /**
     * 是否运行中
     */
    protected volatile boolean running;
    /**
     * Canal 连接器集合
     */
    private List<C> connectors;
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
    private Thread[] workThreads;

    public AbstractCanalClient(List<C> connectors) {
        this.connectors = connectors;
    }

    @Override
    public void start() {
        log.info("start canal client");
        workThreads = new Thread[connectors.size()];
        for (int i = 0; i < connectors.size(); i++) {
            C connector = connectors.get(i);
            Thread workThread = new Thread(() -> process(connector));
            workThread.setName("canal-client-thread-" + i);
            workThreads[i] = workThread;
            workThread.start();
        }
        running = true;
    }

    @Override
    public void stop() {
        log.info("stop canal client");
        running = false;
        for (Thread workThread : workThreads) {
            if (Objects.nonNull(workThread) && workThread.isAlive()){
                workThread.interrupt();
            }
        }
    }

    protected abstract String getDestination(C connector);

    @Override
    public void process(C connector) {
        String destination = this.getDestination(connector);
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
                        messageHandler.handleMessage(destination, message);
                    }
                    connector.ack(batchId);
                }
            } catch (Exception e) {
                log.error("canal client 异常", e);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                }
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

    public MessageHandler getMessageHandler() {
        return messageHandler;
    }

}
