package com.alibaba.otter.canal.spring.boot;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.annotation.CanalEventHandler;
import com.alibaba.otter.canal.annotation.event.*;
import com.alibaba.otter.canal.model.CanalModel;
import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@CanalEventHandler
@Slf4j
public class CanalMessageEventHandler {

    @OnCreateTableEvent(schema = "my_auth")
    public void onCreateTableEvent(CanalModel model, CanalEntry.RowChange rowChange) {
        log.info("onCreateTableEvent");
    }

    @OnCreateTableEvent(schema = "my_auth")
    public void onCreateTableEvent2(CanalModel model, CanalEntry.RowChange rowChange) {
        log.info("onCreateTableEvent2");
    }

    @OnCreateIndexEvent(schema = "my_auth", table = "user_info")
    public void onCreateIndexEvent(CanalModel model, CanalEntry.RowChange rowChange) {
        log.info("OnCreateIndexEvent");
    }

    @OnInsertEvent(schema = "my_auth", table = "user_info")
    public void onEventInsertData(CanalModel model, CanalEntry.RowChange rowChange) {

        // 1，获取当前事件的操作类型
        CanalEntry.EventType eventType = rowChange.getEventType();
        // 2,获取数据集
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
        // 3,遍历RowDataList，并打印数据集
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

            System.out.println("Table:" + model.getTable() +
                    ",EventType:" + eventType +
                    ",Before:" + beforeData +
                    ",After:" + affterData);
        }

    }

    @OnUpdateEvent(schema = "my_auth", table = "user_info")
    public void onEventUpdateData(CanalModel model, CanalEntry.RowChange rowChange) {
        log.info("onEventUpdateData");
    }

    @OnDeleteEvent(schema = "my_auth", table = "user_info")
    public void onEventDeleteData(CanalEntry.RowChange rowChange, CanalModel model) {
        log.info("onEventDeleteData");
    }

}
