package com.alibaba.otter.canal.util;


import com.alibaba.otter.canal.annotation.CanalEventHolder;
import com.alibaba.otter.canal.annotation.CanalTable;
import com.alibaba.otter.canal.annotation.OnCanalEvent;
import com.alibaba.otter.canal.enums.TableNameEnum;
import com.alibaba.otter.canal.handler.EntryHandler;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yang peng
 * @date 2019/3/2713:33
 */
public class HandlerUtil {

    public static EntryHandler getEntryHandler(List<? extends EntryHandler> entryHandlers, String schemaName, String tableName) {
        StringJoiner joiner = new StringJoiner(".").add(schemaName).add(tableName);
        EntryHandler globalHandler = null;
        for (EntryHandler handler : entryHandlers) {
            String canalTableNameCombination = getCanalTableNameCombination(handler);
            if (! StringUtils.hasText(canalTableNameCombination)) {
                continue;
            }
            if (TableNameEnum.ALL.name().toLowerCase().equals(canalTableNameCombination)) {
                globalHandler = handler;
                continue;
            }
            if (canalTableNameCombination.equals(joiner.toString().toLowerCase())) {
                return handler;
            }
            String name = GenericUtil.getTableGenericProperties(handler);
            if (name != null) {
                if (name.equals(tableName)) {
                    return handler;
                }
            }
        }
        return globalHandler;
    }


    public static Map<String, EntryHandler> getTableHandlerMap(List<? extends EntryHandler> entryHandlers) {
        Map<String, EntryHandler> map = new ConcurrentHashMap<>();
        if (CollectionUtils.isEmpty(entryHandlers)) {
            return map;
        }
        for (EntryHandler handler : entryHandlers) {
            String canalTableNameCombination = getCanalTableNameCombination(handler);
            if (StringUtils.hasText(canalTableNameCombination) {
                map.putIfAbsent(canalTableNameCombination.toLowerCase(), handler);
            } else {
                String name = GenericUtil.getTableGenericProperties(handler);
                if (name != null) {
                    map.putIfAbsent(name.toLowerCase(), handler);
                }
            }
        }
        return map;
    }

    public static Map<String, CanalEventHolder> getEventHolderMap(List<CanalEventHolder> eventHolders) {
        Map<String, CanalEventHolder> map = new ConcurrentHashMap<>();
        if (CollectionUtils.isEmpty(eventHolders)) {
            return map;
        }
        for (CanalEventHolder holder : eventHolders) {
            String canalTableNameCombination = getCanalTableNameCombination(holder);
            if (StringUtils.hasText(canalTableNameCombination) {
                map.putIfAbsent(canalTableNameCombination.toLowerCase(), holder);
            }
        }
        return map;
    }

    public static CanalEventHolder getEventHolder(Map<String, CanalEventHolder> map, String schemaName, String tableName) {
        StringJoiner joiner = new StringJoiner(".").add(schemaName).add(tableName);
        CanalEventHolder eventHolder = map.get(joiner.toString().toLowerCase());
        if (eventHolder == null) {
            return map.get(TableNameEnum.ALL.name().toLowerCase());
        }
        return eventHolder;
    }

    public static EntryHandler getEntryHandler(Map<String, EntryHandler> map, String schemaName, String tableName) {
        StringJoiner joiner = new StringJoiner(".").add(schemaName).add(tableName);
        EntryHandler entryHandler = map.get(joiner.toString().toLowerCase());
        if (entryHandler == null) {
            return map.get(TableNameEnum.ALL.name().toLowerCase());
        }
        return entryHandler;
    }


    public static String getCanalTableNameCombination(CanalEventHolder eventHolder) {
        OnCanalEvent canalEvent = eventHolder.getEvent();
        if (Objects.nonNull(canalEvent)) {
            StringJoiner joiner = new StringJoiner(".").add(canalEvent.schema()).add(canalEvent.table());
            return joiner.toString().toLowerCase();
        }
        return null;
    }

    public static String getCanalTableNameCombination(EntryHandler entryHandler) {
        CanalTable canalTable = entryHandler.getClass().getAnnotation(CanalTable.class);
        if (Objects.nonNull(canalTable)) {
            StringJoiner joiner = new StringJoiner(".").add(canalTable.schema()).add(canalTable.table());
            return joiner.toString().toLowerCase();
        }
        return null;
    }

}
