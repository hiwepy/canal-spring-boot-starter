package com.alibaba.otter.canal.handler;

import com.alibaba.otter.canal.annotation.CanalEventHandler;
import com.alibaba.otter.canal.annotation.CanalEventHolder;
import com.alibaba.otter.canal.annotation.OnCanalEvent;
import com.alibaba.otter.canal.context.CanalContext;
import com.alibaba.otter.canal.model.CanalModel;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base {@link MessageHandler} for MQ-based Canal clients that consume flattened
 * {@link FlatMessage} events (JSON).
 * <p>
 * Iterates over the data rows of each flat message, resolves the matching
 * {@link EntryHandler} or annotation-based {@link CanalEventHolder} for the
 * changed table, and dispatches the row to it. For UPDATE events the before-
 * and after-image maps are merged so handlers receive both. Implements
 * {@link ApplicationContextAware} to discover beans annotated with
 * {@link CanalEventHandler} on context initialisation.
 * </p>
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */
public abstract class AbstractFlatMessageHandler implements MessageHandler<FlatMessage>, ApplicationContextAware {

    private static final Logger log = LoggerFactory.getLogger(AbstractFlatMessageHandler.class);

    /** Entry types to subscribe to, marking transaction begin, data change and transaction end. */
    private List<CanalEntry.EntryType> subscribeTypes = Arrays.asList(CanalEntry.EntryType.ROWDATA);
    /** Annotation-based table event handlers keyed by destination/schema/table/event. */
    private Map<String, List<CanalEventHolder>> tableEventHolderMap;
    /** Programmatic {@link EntryHandler} instances keyed by schema/table. */
    private Map<String, EntryHandler> tableHandlerMap;
    /** Row data handler used to transform row data into entry models. */
    private RowDataHandler<List<Map<String, String>>> rowDataHandler;

    /**
     * @param subscribeTypes  entry types to subscribe to (overrides the default when non-null)
     * @param entryHandlers   programmatic entry handlers to register
     * @param rowDataHandler  the row data handler used to transform row data
     */
    public AbstractFlatMessageHandler(List<CanalEntry.EntryType> subscribeTypes,
                                      List<? extends EntryHandler> entryHandlers,
                                      RowDataHandler<List<Map<String, String>>> rowDataHandler) {
        if(Objects.nonNull(subscribeTypes)){
            this.subscribeTypes = subscribeTypes;
        }
        this.tableHandlerMap = HandlerUtil.getTableHandlerMap(entryHandlers);
        this.rowDataHandler = rowDataHandler;
    }

    @Override
    public void handleMessage(String destination, FlatMessage flatMessage) {
        // Skip if there is no data.
        List<Map<String, String>> data = flatMessage.getData();
        if(CollectionUtils.isEmpty(data)){
            return;
        }
        // Iterate over data rows, parsing each one.
        for (int i = 0; i < data.size(); i++) {
            // Database (schema) name.
            String schemaName = flatMessage.getDatabase();
            // Table name.
            String tableName = flatMessage.getTable();
            // Event type.
            CanalEntry.EventType eventType = CanalEntry.EventType.valueOf(flatMessage.getType());
            // Current row data.
            List<Map<String, String>> maps;
            if (eventType.equals(CanalEntry.EventType.UPDATE)) {
                // After-image.
                Map<String, String> map = data.get(i);
                // Before-image.
                Map<String, String> oldMap = flatMessage.getOld().get(i);
                // Merge before/after images.
                maps = Stream.of(map, oldMap).collect(Collectors.toList());
            } else {
                maps = Stream.of(data.get(i)).collect(Collectors.toList());
            }
            try {
                // Resolve annotation-based handlers for this table.
                List<CanalEventHolder> eventHolders = HandlerUtil.getEventHolders(tableEventHolderMap, destination, schemaName, tableName, eventType);
                if(!CollectionUtils.isEmpty(eventHolders)){
                    CanalModel model = CanalModel.builder()
                            .id(flatMessage.getId())
                            .schema(schemaName)
                            .table(tableName)
                            .eventType(eventType)
                            .executeTime(flatMessage.getEs())
                            .createTime(flatMessage.getTs()).build();
                    for (CanalEventHolder eventHolder : eventHolders) {
                        this.handlerRowData(model, maps, eventHolder, eventType);
                    }
                    continue;
                }
                // Resolve the programmatic handler for this table.
                EntryHandler<?> entryHandler = HandlerUtil.getEntryHandler(tableHandlerMap, schemaName, tableName);
                // Dispatch if a matching handler exists.
                if(Objects.nonNull(entryHandler)){
                    CanalModel model = CanalModel.builder()
                            .id(flatMessage.getId())
                            .schema(schemaName)
                            .table(tableName)
                            .eventType(eventType)
                            .executeTime(flatMessage.getEs())
                            .createTime(flatMessage.getTs()).build();
                   this.handlerRowData(model, maps, entryHandler, eventType);
                }
            } catch (Exception e) {
                throw new RuntimeException("parse event has an error , data:" + maps.toString(), e);
            }
        }
    }

    /**
     * Dispatches row data to an annotation-based handler, binding the
     * {@link CanalModel} to the thread-local context for the call.
     *
     * @param model       the Canal context model
     * @param rowData     the row data (column maps) to dispatch
     * @param eventHolder the annotation-based handler holder
     * @param eventType   the Canal event type
     * @throws Exception if the target method invocation fails
     */
    public void handlerRowData(CanalModel model, List<Map<String, String>> rowData, CanalEventHolder eventHolder, CanalEntry.EventType eventType) throws Exception {
        Method method = eventHolder.getMethod();
        try {
            CanalContext.setModel(model);
            ReflectionUtils.makeAccessible(method);
            Object[] args = GenericUtil.getInvokeArgs(method, model, rowData, eventType);
            method.invoke(eventHolder.getTarget(), args);
        } finally {
            // Clear the thread-local context.
            CanalContext.removeModel();
        }
    }

    /**
     * Dispatches row data to a programmatic {@link EntryHandler} via the
     * configured {@link RowDataHandler}, binding the {@link CanalModel} to the
     * thread-local context for the call.
     *
     * @param model        the Canal context model
     * @param rowData      the row data (column maps) to dispatch
     * @param entryHandler the programmatic entry handler
     * @param eventType    the Canal event type
     * @throws Exception if the row data handler fails
     */
    public void handlerRowData(CanalModel model, List<Map<String, String>> rowData, EntryHandler entryHandler, CanalEntry.EventType eventType) throws Exception {
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
