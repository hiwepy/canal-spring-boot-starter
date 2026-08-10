package com.alibaba.otter.canal.handler;

import com.alibaba.otter.canal.annotation.CanalEventHandler;
import com.alibaba.otter.canal.annotation.CanalEventHolder;
import com.alibaba.otter.canal.annotation.OnCanalEvent;
import com.alibaba.otter.canal.context.CanalContext;
import com.alibaba.otter.canal.model.CanalModel;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.util.GenericUtil;
import com.alibaba.otter.canal.util.HandlerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.*;


/**
 * Base {@link MessageHandler} for direct (non-MQ) Canal clients that consume
 * protobuf {@link Message} batches.
 * <p>
 * Iterates over the entries of each message, resolves the matching
 * {@link EntryHandler} or annotation-based {@link CanalEventHolder} for the
 * changed table, and dispatches every {@code RowData} to it. Implements
 * {@link ApplicationContextAware} to discover beans annotated with
 * {@link CanalEventHandler} on context initialisation.
 * </p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public abstract class AbstractMessageHandler implements MessageHandler<Message>, ApplicationContextAware {

    private static final Logger log = LoggerFactory.getLogger(AbstractMessageHandler.class);

    /** Entry types to subscribe to, marking transaction begin, data change and transaction end. */
    private List<CanalEntry.EntryType> subscribeTypes = Arrays.asList(CanalEntry.EntryType.ROWDATA);
    /** Annotation-based table event handlers keyed by destination/schema/table/event. */
    private Map<String, List<CanalEventHolder>> tableEventHolderMap;
    /** Programmatic {@link EntryHandler} instances keyed by schema/table. */
    private Map<String, EntryHandler> tableHandlerMap;
    /** Row data handler used to transform row data into entry models. */
    private RowDataHandler<CanalEntry.RowData> rowDataHandler;

    /**
     * @param subscribeTypes  entry types to subscribe to (overrides the default when non-null)
     * @param entryHandlers   programmatic entry handlers to register
     * @param rowDataHandler  the row data handler used to transform row data
     */
    public AbstractMessageHandler(List<CanalEntry.EntryType> subscribeTypes,
                                  List<? extends EntryHandler> entryHandlers,
                                  RowDataHandler<CanalEntry.RowData> rowDataHandler) {
        if(Objects.nonNull(subscribeTypes)){
            this.subscribeTypes = subscribeTypes;
        }
        this.tableHandlerMap = HandlerUtil.getTableHandlerMap(entryHandlers);
        this.rowDataHandler = rowDataHandler;
    }

    /**
     * @param entryType the entry type to test
     * @return {@code true} if the entry type is among the subscribed types
     */
    protected boolean isSubscribed(CanalEntry.EntryType entryType) {
        return subscribeTypes.contains(entryType);
    }

    @Override
    public void handleMessage(String destination, Message message) {
        // Iterate over entries, parsing each one.
        for (CanalEntry.Entry entry : message.getEntries()) {
            // Resolve the entry type.
            CanalEntry.EntryType entryType = entry.getEntryType();
            // Check whether this entry type is subscribed.
            if (this.isSubscribed(entryType)) {
                // Database (schema) name.
                String schemaName = entry.getHeader().getSchemaName();
                // Table name.
                String tableName = entry.getHeader().getTableName();
                try {
                    // Deserialise the stored value into a RowChange.
                    CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    // Event type of the current change.
                    CanalEntry.EventType eventType = rowChange.getEventType();
                    // Resolve annotation-based handlers for this table.
                    List<CanalEventHolder> eventHolders = HandlerUtil.getEventHolders(tableEventHolderMap, destination, schemaName, tableName, eventType);
                    if(!CollectionUtils.isEmpty(eventHolders)){
                        CanalModel model = CanalModel.builder()
                                .id(message.getId())
                                .schema(schemaName)
                                .table(tableName)
                                .eventType(eventType)
                                .executeTime(entry.getHeader().getExecuteTime())
                                .build();
                        for (CanalEventHolder eventHolder : eventHolders) {
                            this.handlerRowData(model, rowChange, eventHolder, eventType);
                        }
                        continue;
                    }
                    // Resolve the programmatic handler for this table.
                    EntryHandler<?> entryHandler = HandlerUtil.getEntryHandler(tableHandlerMap, schemaName, tableName);
                    // Dispatch if a matching handler exists.
                    if(Objects.nonNull(entryHandler)){
                        CanalModel model = CanalModel.builder()
                                .id(message.getId())
                                .schema(schemaName)
                                .table(tableName)
                                .eventType(eventType)
                                .executeTime(entry.getHeader().getExecuteTime())
                                .build();
                        // Iterate over the row data list, dispatching each row.
                        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                            this.handlerRowData(model, rowData, entryHandler, eventType);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }
            } else {
                log.info("current operation type is: {}", entryType);
            }
        }
    }

    /**
     * Dispatches a {@link CanalEntry.RowChange} to an annotation-based handler,
     * binding the {@link CanalModel} to the thread-local context for the call.
     *
     * @param model        the Canal context model
     * @param rowChange    the row change to dispatch
     * @param eventHolder  the annotation-based handler holder
     * @param eventType    the Canal event type
     * @throws Exception if the target method invocation fails
     */
    public void handlerRowData(CanalModel model, CanalEntry.RowChange rowChange, CanalEventHolder eventHolder, CanalEntry.EventType eventType) throws Exception {
        try {
            CanalContext.setModel(model);
            Method method = eventHolder.getMethod();
            ReflectionUtils.makeAccessible(method);
            Object[] args = GenericUtil.getInvokeArgs(method, model, rowChange, eventType);
            method.invoke(eventHolder.getTarget(), args);
        } finally {
            // Clear the thread-local context.
            CanalContext.removeModel();
        }
    }

    /**
     * Dispatches a single {@link CanalEntry.RowData} to a programmatic
     * {@link EntryHandler} via the configured {@link RowDataHandler}, binding
     * the {@link CanalModel} to the thread-local context for the call.
     *
     * @param model        the Canal context model
     * @param rowData      the row data to dispatch
     * @param entryHandler the programmatic entry handler
     * @param eventType    the Canal event type
     * @throws Exception if the row data handler fails
     */
    public void handlerRowData(CanalModel model, CanalEntry.RowData rowData, EntryHandler entryHandler, CanalEntry.EventType eventType) throws Exception {
        try {
            // Bind the context.
            CanalContext.setModel(model);
            // Dispatch the row to the handler.
            rowDataHandler.handlerRowData(rowData, entryHandler, eventType);
        } finally {
            // Clear the thread-local context.
            CanalContext.removeModel();
        }
    }

    /**
     * Discovers beans annotated with {@link CanalEventHandler} and indexes
     * their {@link OnCanalEvent} methods into {@link #tableEventHolderMap}.
     *
     * @param applicationContext the running application context
     * @throws BeansException if bean lookup fails
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        log.info("{}: annotation event handler is initializing....", Thread.currentThread().getName());
        // Collect every @CanalEventHandler bean.
        Map<String, Object> eventHandlerMap = applicationContext.getBeansWithAnnotation(CanalEventHandler.class);
        if(CollectionUtils.isEmpty(eventHandlerMap)){
            log.info("{}: not found annotation event handler.", Thread.currentThread().getName());
            return;
        }
        // Build the event-holder index.
        List<CanalEventHolder> eventHolders = new ArrayList<>();
        for (Object target : eventHandlerMap.values()) {
            // Inspect every declared method on the bean.
            Method[] methods = ReflectionUtils.getDeclaredMethods(target.getClass());
            for (Method method : methods) {
                OnCanalEvent canalEvent = AnnotatedElementUtils.findMergedAnnotation(method, OnCanalEvent.class);
                if (Objects.nonNull(canalEvent)) {
                    eventHolders.add(new CanalEventHolder(target, method, canalEvent));
                }
            }
        }
        this.tableEventHolderMap = HandlerUtil.getEventHolderMap(eventHolders);
        log.info("{}: annotation event handler initialized finish.", Thread.currentThread().getName());
    }

}
