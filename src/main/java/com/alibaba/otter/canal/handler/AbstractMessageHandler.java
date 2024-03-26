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
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.*;

/**
 * @author yang peng
 * @date 2019/3/2921:38
 */
@Slf4j
public abstract class AbstractMessageHandler implements MessageHandler<Message>, ApplicationContextAware {

    /**
     * 指定订阅的事件类型，主要用于标识事务的开始，变更数据，结束
     */
    private List<CanalEntry.EntryType> subscribeTypes = Arrays.asList(CanalEntry.EntryType.ROWDATA);
    /**
     * 通过注解方式的表数据变更处理器
     */
    private Map<String, CanalEventHolder> tableEventHolderMap;
    /**
     * 表数据变更处理器
     */
    private Map<String, EntryHandler> tableHandlerMap;
    /**
     * 行数据处理器
     */
    private RowDataHandler<CanalEntry.RowData> rowDataHandler;

    public AbstractMessageHandler(List<CanalEntry.EntryType> subscribeTypes,
                                  List<? extends EntryHandler> entryHandlers,
                                  RowDataHandler<CanalEntry.RowData> rowDataHandler) {
        if(Objects.nonNull(subscribeTypes)){
            this.subscribeTypes = subscribeTypes;
        }
        this.tableHandlerMap = HandlerUtil.getTableHandlerMap(entryHandlers);
        this.rowDataHandler = rowDataHandler;
    }

    protected boolean isSubscribed(CanalEntry.EntryType entryType) {
        return subscribeTypes.contains(entryType);
    }

    @Override
    public void handleMessage(Message message) {
        // 遍历 entryes，单条解析
        for (CanalEntry.Entry entry : message.getEntries()) {
            // 获取数据库实例
            String schemaName = entry.getHeader().getSchemaName();
            // 获取表名
            String tableName = entry.getHeader().getTableName();
            // 获取类型
            CanalEntry.EntryType entryType = entry.getEntryType();
            // 判断当前entryType类型是否订阅
            if (this.isSubscribed(entryType)) {
                try {
                    // 设置上下文
                    CanalModel model = CanalModel.Builder.builder()
                            .id(message.getId())
                            .table(tableName)
                            .executeTime(entry.getHeader().getExecuteTime())
                            .database(schemaName)
                            .build();
                    // 获取序列化后的数据
                    CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    // 获取当前事件的操作类型
                    CanalEntry.EventType eventType = rowChange.getEventType();
                    // 获取表对应的注解处理器
                    CanalEventHolder eventHolder = HandlerUtil.getEventHolder(tableEventHolderMap, schemaName, tableName);
                    if(Objects.nonNull(eventHolder) && eventHolder.isMatch(eventType)){
                        this.handlerRowData(model, rowChange, eventHolder);
                        continue;
                    }
                    // 获取表对应的处理器
                    EntryHandler<?> entryHandler = HandlerUtil.getEntryHandler(tableHandlerMap, schemaName, tableName);
                    // 判断是否有对应的处理器
                    if(Objects.nonNull(entryHandler)){
                        // 遍历RowDataList，并逐行调用Handler处理
                        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                            this.handlerRowData(model, rowData, entryHandler, eventType);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }
            } else {
                log.info("当前操作类型为：{}", entryType);
            }
        }
    }

    public void handlerRowData(CanalModel model, CanalEntry.RowChange rowChange, CanalEventHolder eventHolder) throws Exception {
        try {
            CanalContext.setModel(model);
            Method method = eventHolder.getMethod();
            ReflectionUtils.makeAccessible(method);
            Object[] args = GenericUtil.getInvokeArgs(method, model, rowChange);
            method.invoke(eventHolder.getTarget(), args);
        } finally {
            // 移除上下文
            CanalContext.removeModel();
        }
    }

    public void handlerRowData(CanalModel model, CanalEntry.RowData rowData, EntryHandler entryHandler, CanalEntry.EventType eventType) throws Exception {
        try {
            // 设置上下文
            CanalContext.setModel(model);
            // 逐行调用Handler处理
            rowDataHandler.handlerRowData(rowData, entryHandler, eventType);
        } finally {
            // 移除上下文
            CanalContext.removeModel();
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        log.info("{}: annotation event handler is initializing....", Thread.currentThread().getName());
        // 获取所有的处理器
        Map<String, Object> eventHandlerMap = applicationContext.getBeansWithAnnotation(CanalEventHandler.class);
        if(CollectionUtils.isEmpty(eventHandlerMap)){
            log.info("{}: not found annotation event handler.", Thread.currentThread().getName());
            return;
        }
        // 注解处理器对象
        List<CanalEventHolder> eventHolders = new ArrayList<>();
        for (Object target : eventHandlerMap.values()) {
            // 获取对象声明的方法
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
