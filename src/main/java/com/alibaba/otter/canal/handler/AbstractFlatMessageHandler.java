package com.alibaba.otter.canal.handler;

import com.alibaba.otter.canal.context.CanalContext;
import com.alibaba.otter.canal.model.CanalModel;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.util.HandlerUtil;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractFlatMessageHandler implements MessageHandler<FlatMessage> {

    /**
     * 表处理器
     */
    private Map<String, EntryHandler> tableHandlerMap;
    /**
     * 行数据处理器
     */
    private RowDataHandler<List<Map<String, String>>> rowDataHandler;

    public AbstractFlatMessageHandler(List<? extends EntryHandler> entryHandlers, RowDataHandler<List<Map<String, String>>> rowDataHandler) {
        this.tableHandlerMap = HandlerUtil.getTableHandlerMap(entryHandlers);
        this.rowDataHandler = rowDataHandler;
    }

    @Override
    public void handleMessage(FlatMessage flatMessage) {
        // 判断是否有 Data
        List<Map<String, String>> data = flatMessage.getData();
        if(CollectionUtils.isEmpty(data)){
            return;
        }
        // 遍历 Data，单条解析
        for (int i = 0; i < data.size(); i++) {
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
                // 获取表对应的处理器
                EntryHandler<?> entryHandler = HandlerUtil.getEntryHandler(tableHandlerMap, tableName);
                // 判断是否有对应的处理器
                if(Objects.nonNull(entryHandler)){
                    // 设置上下文
                    CanalModel model = CanalModel.Builder.builder()
                            .id(flatMessage.getId())
                            .table(tableName)
                            .executeTime(flatMessage.getEs())
                            .database(flatMessage.getDatabase())
                            .createTime(flatMessage.getTs()).build();
                    CanalContext.setModel(model);
                    // 逐行调用Handler处理
                    rowDataHandler.handlerRowData(maps, entryHandler, eventType);
                }
            } catch (Exception e) {
                throw new RuntimeException("parse event has an error , data:" + maps.toString(), e);
            } finally {
                // 移除上下文
                CanalContext.removeModel();
            }
        }
    }

}
