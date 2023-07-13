package com.alibaba.otter.canal.spring.boot.utils;


import com.alibaba.otter.canal.spring.boot.annotation.CanalTable;
import com.alibaba.otter.canal.spring.boot.enums.TableNameEnum;
import com.alibaba.otter.canal.spring.boot.handler.EntryHandler;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yang peng
 * @date 2019/3/2713:33
 */
public class HandlerUtil {

    public static EntryHandler getEntryHandler(List<? extends EntryHandler> entryHandlers, String tableName) {
        EntryHandler globalHandler = null;
        for (EntryHandler handler : entryHandlers) {
            String canalTableName = getCanalTableName(handler);
            if (TableNameEnum.ALL.name().toLowerCase().equals(canalTableName)) {
                globalHandler = handler;
                continue;
            }
            if (tableName.equals(canalTableName)) {
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
        if (!CollectionUtils.isEmpty(entryHandlers)) {
            for (EntryHandler handler : entryHandlers) {
                String canalTableName = getCanalTableName(handler);
                if (StringUtils.hasText(canalTableName)) {
                    map.putIfAbsent(canalTableName.toLowerCase(), handler);
                } else {
                    String tableName = GenericUtil.getTableGenericProperties(handler);
                    if (StringUtils.hasText(tableName)) {
                        map.putIfAbsent(tableName.toLowerCase(), handler);
                    }
                }
            }
        }
        return map;
    }


    public static EntryHandler getEntryHandler(Map<String, EntryHandler> map, String tableName) {
        EntryHandler entryHandler = map.get(tableName);
        if (entryHandler == null) {
            return map.get(TableNameEnum.ALL.name().toLowerCase());
        }
        return entryHandler;
    }

    public static String getCanalTableName(EntryHandler entryHandler) {
        CanalTable canalTable = entryHandler.getClass().getAnnotation(CanalTable.class);
        if (canalTable != null) {
            return canalTable.value();
        }
        return null;
    }

}
