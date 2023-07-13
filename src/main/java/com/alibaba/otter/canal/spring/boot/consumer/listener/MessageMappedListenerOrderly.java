package com.alibaba.otter.canal.spring.boot.consumer.listener;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.handler.EntryHandler;
import com.alibaba.otter.canal.spring.boot.handler.RowDataHandler;
import com.alibaba.otter.canal.spring.boot.utils.HandlerUtil;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MessageMappedListenerOrderly implements MessageListenerOrderly {

    private Map<String, EntryHandler> tableHandlerMap;

    private RowDataHandler<List<Map<String, String>>> rowDataHandler;

    @Override
    public ConsumeOrderlyStatus consumeMessage(List<Message> messages) throws Exception {
        for (Message message : messages) {
            try {
                EntryHandler<?> entryHandler = HandlerUtil.getEntryHandler(tableHandlerMap, message.getTable());
                if (entryHandler != null) {
                    CanalModel model = CanalModel.Builder.builder().id(message.getId()).table(message.getTable())
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
        return null;
    }

    @Override
    public ConsumeOrderlyStatus consumeFlatMessage(List<FlatMessage> messages) throws Exception {
        for (FlatMessage message : messages) {
            List<Map<String, String>> data = message.getData();
            if (data != null && data.size() > 0) {
                for (int i = 0; i < data.size(); i++) {
                    CanalEntry.EventType eventType = CanalEntry.EventType.valueOf(message.getType());
                    List<Map<String, String>> maps;
                    if (eventType.equals(CanalEntry.EventType.UPDATE)) {
                        Map<String, String> map = data.get(i);
                        Map<String, String> oldMap = message.getOld().get(i);
                        maps = Stream.of(map, oldMap).collect(Collectors.toList());
                    } else {
                        maps = Stream.of(data.get(i)).collect(Collectors.toList());
                    }
                    try {
                        EntryHandler<?> entryHandler = HandlerUtil.getEntryHandler(tableHandlerMap, message.getTable());
                        if (entryHandler != null) {
                            CanalModel model = CanalModel.Builder.builder().id(message.getId()).table(message.getTable())
                                    .executeTime(message.getEs()).database(message.getDatabase()).createTime(message.getTs()).build();
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
        return null;
    }

}
