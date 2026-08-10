package com.alibaba.otter.canal.client;

import com.alibaba.otter.canal.handler.MessageHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.util.CanalUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.slf4j.MDC;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Base implementation of a Canal client that consumes binlog entries directly
 * from one or more {@link CanalConnector} instances.
 * <p>
 * On {@link #start()} a worker thread is spawned per connector; each thread
 * subscribes, polls for messages without ack, dispatches them to the configured
 * {@link MessageHandler}, and then acks or rolls back the batch depending on
 * whether processing succeeds. Failures cause the batch to be rolled back and
 * consumption to resume after a short back-off.
 * </p>
 *
 * @param <C> the {@link CanalConnector} implementation type
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@Slf4j
public abstract class AbstractCanalClient<C extends CanalConnector> implements CanalClient<C> {

    /** Handler invoked when a worker thread terminates with an uncaught exception. */
    protected Thread.UncaughtExceptionHandler handler            = (t, e) -> log.error("parse events has an error",
            e);
    /** Whether the client is currently running and its worker threads are active. */
    protected volatile boolean running;
    /** Connectors that this client consumes from; one worker thread is created per connector. */
    private List<C> connectors;
    /** Canal subscription filter expression. */
    protected String filter = StringUtils.EMPTY;
    /** Number of messages fetched per poll. */
    protected Integer batchSize = 1;
    /** Polling timeout; {@code -1} disables timeout control. */
    protected Long timeout = -1L;
    /** Time unit applied to {@link #timeout}. */
    protected TimeUnit unit = TimeUnit.SECONDS;
    /** Entry types to subscribe to, marking transaction begin, data change and transaction end. */
    protected List<CanalEntry.EntryType> subscribeTypes = Arrays.asList(CanalEntry.EntryType.ROWDATA);
    /** Handler that receives each polled Canal message. */
    private MessageHandler messageHandler;
    /** Worker threads, one per connector. */
    private Thread[] workThreads;

    /**
     * @param connectors the connectors this client will consume from
     */
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
            workThread.setUncaughtExceptionHandler(handler);
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

    /**
     * Resolves the Canal destination name for the given connector, used for logging context.
     *
     * @param connector the connector to inspect
     * @return the destination name
     */
    protected abstract String getDestination(C connector);

    @Override
    public void process(C connector) {
        String destination = this.getDestination(connector);
        while (running) {
            try {
                MDC.put("destination", destination);
                connector.connect();
                connector.subscribe(filter);
                while (running) {
                    Message message = connector.getWithoutAck(batchSize, timeout, unit);
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                         try {
                            Thread.sleep(1000);
                         } catch (InterruptedException e) {
                         }
                    } else {
                        CanalUtils.printSummary(message, batchId, size);
                        CanalUtils.printEntry(message.getEntries());
                        messageHandler.handleMessage(destination, message);
                    }

                    if (batchId != -1) {
                        connector.ack(batchId); // Acknowledge the processed batch.
                    }

                }
            } catch (Exception e) {
                log.error("process error!", e);
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException e1) {
                    // ignore
                }
                connector.rollback(); // Processing failed: roll back the batch.
            } finally {
                connector.disconnect();
            }
        }
    }

    @Override
    public void destroy() throws Exception {
        stop();
    }

    /** @param batchSize number of messages fetched per poll */
    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    /** @param filter Canal subscription filter expression */
    public void setFilter(String filter) {
        this.filter = filter;
    }

    /** @param messageHandler handler that receives each polled message */
    public void setMessageHandler(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    /** @param timeout polling timeout; {@code -1} disables timeout control */
    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    /** @param unit time unit applied to {@link #timeout} */
    public void setUnit(TimeUnit unit) {
        this.unit = unit;
    }

    /** @param subscribeTypes entry types to subscribe to */
    public void setSubscribeTypes(List<CanalEntry.EntryType> subscribeTypes) {
        this.subscribeTypes = subscribeTypes;
    }

    /** @return the handler that receives each polled message */
    public MessageHandler getMessageHandler() {
        return messageHandler;
    }

}
