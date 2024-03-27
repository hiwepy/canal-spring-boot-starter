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
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
public abstract class AbstractFlatMessageHandler implements MessageHandler<FlatMessage>, ApplicationContextAware {

    /**
     * 指定订阅的事件类型，主要用于标识事务的开始，变更数据，结束
     */
    private List<CanalEntry.EntryType> subscribeTypes = Arrays.asList(CanalEntry.EntryType.ROWDATA);
    /**
     * 通过注解方式的表数据变更处理器
     */
    private Map<String, List<CanalEventHolder>> tableEventHolderMap;
    /**
     * 表处理器
     */
    private Map<String, EntryHandler> tableHandlerMap;
    /**
     * 行数据处理器
     */
    private RowDataHandler<List<Map<String, String>>> rowDataHandler;

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
        // 判断是否有 Data
        List<Map<String, String>> data = flatMessage.getData();
        if(CollectionUtils.isEmpty(data)){
            return;
        }
        // 遍历 Data，单条解析
        for (int i = 0; i < data.size(); i++) {
            // 获取数据库实例
            String schemaName = flatMessage.getDatabase();
            // 获取表名
            String tableName = flatMessage.getTable();
            // 获取类型
            CanalEntry.EventType eventType = CanalEntry.EventType.valueOf(flatMessage.getType());
            // 获取当前行数据
            List<Map<String, String>> maps;
            if (eventType.equals(CanalEntry.EventType.UPDATE)) {
                // 更新后的数据
                Map<String, String> map = data.get(i);
                // 更新前的数据
                Map<String, String> oldMap = flatMessage.getOld().get(i);
                // 合并新旧数据
                maps = Stream.of(map, oldMap).collect(Collectors.toList());
            } else {
                maps = Stream.of(data.get(i)).collect(Collectors.toList());
            }
            try {
                // 获取表对应的注解处理器
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
                // 获取表对应的处理器
                EntryHandler<?> entryHandler = HandlerUtil.getEntryHandler(tableHandlerMap, schemaName, tableName);
                // 判断是否有对应的处理器
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

    public void handlerRowData(CanalModel model, List<Map<String, String>> rowData, CanalEventHolder eventHolder, CanalEntry.EventType eventType) throws Exception {
        Method method = eventHolder.getMethod();
        try {
            CanalContext.setModel(model);
            ReflectionUtils.makeAccessible(method);
            Object[] args = GenericUtil.getInvokeArgs(method, model, rowData, eventType);
            method.invoke(eventHolder.getTarget(), args);
        } finally {
            // 移除上下文
            CanalContext.removeModel();
        }
    }

    public void handlerRowData(CanalModel model, List<Map<String, String>> rowData, EntryHandler entryHandler, CanalEntry.EventType eventType) throws Exception {
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
