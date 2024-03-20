package com.alibaba.otter.canal.spring.boot.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import top.javatool.canal.client.handler.MessageHandler;

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

    public C getConnector() {
        return connector;
    }

    public MessageHandler getMessageHandler() {
        return messageHandler;
    }

}
