package com.alibaba.otter.canal.spring.boot;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.otter.canal.spring.boot.consumer.listener.MessageListenerConcurrently;
import com.alibaba.otter.canal.spring.boot.utils.CanalUtils;
import com.google.protobuf.ByteString;

import java.util.List;

public class CanalMessageListenerConcurrently  implements MessageListenerConcurrently {

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<Message> messages) throws Exception {
        // 循环所有消息
        for (Message message: messages) {
            // 1、获取 Entry集合
            List<CanalEntry.Entry> entries = message.getEntries();
            long batchId = message.getId();
            CanalUtils.printSummary(message, batchId, entries.size());
            if (batchId == -1 || entries.size() == 0) {
                System.out.println("休息一会吧，当前抓取没有数据");
            } else {
                CanalUtils.printEntry(message.getEntries());
                // 遍历 entryes，单条解析
                for (CanalEntry.Entry entry : entries) {
                    //1，获取表名
                    String tableName = entry.getHeader().getTableName();
                    //2，获取类型
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    //3,获取序列化后的数据
                    ByteString storeValue = entry.getStoreValue();
                    //4,判断当前entryType类型是否为ROWDATA，既当前变化的数据是否行数据
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        //5,反序列化数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //6，获取当前事件的操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //7,获取数据集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //8,遍历RowDataList，并打印数据集
                        for (CanalEntry.RowData rowData : rowDatasList) {
                            JSONObject beforeData = new JSONObject();
                            List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                            for (CanalEntry.Column column : beforeColumnsList) {
                                beforeData.put(column.getName(), column.getValue());
                            }
                            JSONObject affterData = new JSONObject();
                            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                            for (CanalEntry.Column column : afterColumnsList) {
                                affterData.put(column.getName(), column.getValue());
                            }

                            System.out.println("Table:" + tableName +
                                    ",EventType:" + eventType +
                                    ",Before:" + beforeData +
                                    ",After:" + affterData);
                        }

                    } else {
                        System.out.println("当前操作类型为：" + entryType);
                    }
                }
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

}
