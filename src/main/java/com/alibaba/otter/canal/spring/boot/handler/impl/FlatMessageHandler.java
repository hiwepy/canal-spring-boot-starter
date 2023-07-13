package com.alibaba.otter.canal.spring.boot.handler.impl;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.spring.boot.handler.EntryHandler;
import com.alibaba.otter.canal.spring.boot.handler.MessageHandler;
import com.alibaba.otter.canal.spring.boot.handler.RowDataHandler;
import com.alibaba.otter.canal.spring.boot.utils.HandlerUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FlatMessageHandler implements MessageHandler<FlatMessage> {

    private Map<String, EntryHandler> tableHandlerMap;

    private RowDataHandler<List<Map<String, String>>> rowDataHandler;


    public FlatMessageHandler(List<? extends EntryHandler> entryHandlers, RowDataHandler<List<Map<String, String>>> rowDataHandler) {
        this.tableHandlerMap = HandlerUtil.getTableHandlerMap(entryHandlers);
        this.rowDataHandler = rowDataHandler;
    }

    @Override
    public void handleMessage(FlatMessage flatMessage) {
        List<Map<String, String>> data = flatMessage.getData();
        if (data != null && data.size() > 0) {
            for (int i = 0; i < data.size(); i++) {
                CanalEntry.EventType eventType = CanalEntry.EventType.valueOf(flatMessage.getType());
                List<Map<String, String>> maps;
                if (eventType.equals(CanalEntry.EventType.UPDATE)) {
                    Map<String, String> map = data.get(i);
                    Map<String, String> oldMap = flatMessage.getOld().get(i);
                    maps = Stream.of(map, oldMap).collect(Collectors.toList());
                } else {
                    maps = Stream.of(data.get(i)).collect(Collectors.toList());
                }
                try {
                    EntryHandler<?> entryHandler = HandlerUtil.getEntryHandler(tableHandlerMap, flatMessage.getTable());
                    if (entryHandler != null) {
                        CanalModel model = CanalModel.Builder.builder().id(flatMessage.getId()).table(flatMessage.getTable())
                                .executeTime(flatMessage.getEs()).database(flatMessage.getDatabase()).createTime(flatMessage.getTs()).build();
                        CanalContext.setModel(model);
                        rowDataHandler.handlerRowData(maps, entryHandler, eventType);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + data.toString(), e);
                } finally {
                    CanalContext.removeModel();
                }
            }
        }
    }


}
