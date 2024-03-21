package com.alibaba.otter.canal.handler;

import com.alibaba.otter.canal.context.CanalContext;
import com.alibaba.otter.canal.model.CanalModel;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.util.HandlerUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author yang peng
 * @date 2019/3/2921:38
 */
@Slf4j
public abstract class AbstractMessageHandler implements MessageHandler<Message> {

    /**
     * 表处理器
     */
    private Map<String, EntryHandler> tableHandlerMap;
    /**
     * 行数据处理器
     */
    private RowDataHandler<CanalEntry.RowData> rowDataHandler;

    public  AbstractMessageHandler(List<? extends EntryHandler> entryHandlers, RowDataHandler<CanalEntry.RowData> rowDataHandler) {
        this.tableHandlerMap = HandlerUtil.getTableHandlerMap(entryHandlers);
        this.rowDataHandler = rowDataHandler;
    }

    @Override
    public  void handleMessage(Message message) {
        // 遍历 entryes，单条解析
        for (CanalEntry.Entry entry : message.getEntries()) {
            // 获取表名
            String tableName = entry.getHeader().getTableName();
            // 获取类型
            CanalEntry.EntryType entryType = entry.getEntryType();
            // 判断当前entryType类型是否为ROWDATA，既当前变化的数据是否行数据
            if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                try {
                    // 获取表对应的处理器
                    EntryHandler<?> entryHandler = HandlerUtil.getEntryHandler(tableHandlerMap, tableName);
                    // 判断是否有对应的处理器
                    if(Objects.nonNull(entryHandler)){
                        // 设置上下文
                        CanalModel model = CanalModel.Builder.builder()
                                .id(message.getId())
                                .table(tableName)
                                .executeTime(entry.getHeader().getExecuteTime())
                                .database(entry.getHeader().getSchemaName())
                                .build();
                        CanalContext.setModel(model);
                        // 获取序列化后的数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        // 获取当前事件的操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        // 遍历RowDataList，并逐行调用Handler处理
                        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                            rowDataHandler.handlerRowData(rowData, entryHandler, eventType);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                } finally {
                    // 移除上下文
                    CanalContext.removeModel();
                }
            } else {
                log.info("当前操作类型为：{}", entryType);
            }
        }
    }


}
