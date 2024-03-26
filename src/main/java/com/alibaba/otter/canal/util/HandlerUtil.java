package com.alibaba.otter.canal.util;


import com.alibaba.otter.canal.annotation.CanalEventHolder;
import com.alibaba.otter.canal.annotation.CanalTable;
import com.alibaba.otter.canal.annotation.OnCanalEvent;
import com.alibaba.otter.canal.enums.TableNameEnum;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author yang peng
 * @date 2019/3/2713:33
 */
public class HandlerUtil {

    protected static Map<String, Predicate<CanalEventHolder>> eventPredicateMap = new ConcurrentHashMap<>();

    public static EntryHandler getEntryHandler(List<? extends EntryHandler> entryHandlers, String schemaName, String tableName) {
        StringJoiner joiner = new StringJoiner(".").add(schemaName).add(tableName);
        EntryHandler globalHandler = null;
        for (EntryHandler handler : entryHandlers) {
            String canalTableNameCombination = getCanalTableNameCombination(handler);
            if (StringUtils.isBlank(canalTableNameCombination)) {
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
            if (StringUtils.isNotBlank(canalTableNameCombination)) {
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

    /**
     * 获取事件处理器Map, 此方法会将事件处理器按照 destination,schema,table,eventType 的拼接值进行分组
     * @param eventHolders
     * @return
     */
    public static Map<String, List<CanalEventHolder>> getEventHolderMap(List<CanalEventHolder> eventHolders) {
        Map<String, List<CanalEventHolder>> map = new ConcurrentHashMap<>();
        if (CollectionUtils.isEmpty(eventHolders)) {
            return map;
        }
        for (CanalEventHolder holder : eventHolders) {
            List<String> canalTableNameCombinations = getCanalTableNameCombinations(holder);
            if (CollectionUtils.isEmpty(canalTableNameCombinations)) {
                continue;
            }
            for (String canalTableNameCombination : canalTableNameCombinations) {
                map.computeIfAbsent(canalTableNameCombination, k -> new ArrayList<>()).add(holder);
            }
        }
        return map;
    }

    public static List<CanalEventHolder> getEventHolders(Map<String, List<CanalEventHolder>> map,
                                                        String destination,
                                                        String schemaName,
                                                        String tableName,
                                                        CanalEntry.EventType eventType) {
        // 获取四个属性的拼接值
        String key = getCombinationValue(destination, schemaName, tableName, eventType);
        // 获取唯一值对应的过滤器
        Predicate<CanalEventHolder> predicate =  eventPredicateMap.computeIfAbsent(key, k -> getAnnotationFilter(destination, schemaName, tableName, eventType));
        // 返回过滤后的结果
        return map.getOrDefault(key, Collections.emptyList()).stream().filter(predicate).collect(Collectors.toList());
    }

    public static EntryHandler getEntryHandler(Map<String, EntryHandler> map, String schemaName, String tableName) {
        StringJoiner joiner = new StringJoiner(".").add(schemaName).add(tableName);
        EntryHandler entryHandler = map.get(joiner.toString().toLowerCase());
        if (entryHandler == null) {
            return map.get(TableNameEnum.ALL.name().toLowerCase());
        }
        return entryHandler;
    }

    /**
     * 获取注解过滤器
     * @param destination canal 指令
     * @param schemaName 数据库实例
     * @param tableName 表名
     * @param eventType 事件类型
     * @return 过滤器
     */
    protected static Predicate<CanalEventHolder> getAnnotationFilter(String destination,
                                                                     String schemaName,
                                                                     String tableName,
                                                                     CanalEntry.EventType eventType) {

        // 比较 destination 是否一致，如果没有指定 destination 则默认为所有
        Predicate<CanalEventHolder> df = holder -> StringUtils.isEmpty(holder.getEvent().destination())
                || holder.getEvent().destination().equals(destination) || destination == null;

        // 比较数据库实例名是否一致
        Predicate<CanalEventHolder> sf = holder -> StringUtils.isNotBlank(holder.getEvent().schema())
                && holder.getEvent().schema().equalsIgnoreCase(schemaName);

        // 比较表名是否一致，如果没有指定表名则默认为所有
        Predicate<CanalEventHolder> tf = holder -> StringUtils.isNotBlank(holder.getEvent().table())
                && ( holder.getEvent().table().equalsIgnoreCase(tableName) || holder.getEvent().table().equals(TableNameEnum.ALL.getTable()) );

        // 比较事件类型是否一致
        Predicate<CanalEventHolder> ef = holder -> holder.getEvent().eventType().length > 0 && Arrays.stream(holder.getEvent().eventType()).anyMatch(ev -> ev == eventType) ;

        return df.and(sf).and(tf).and(ef);
    }

    public static String getCanalTableNameCombination(EntryHandler entryHandler) {
        CanalTable canalTable = entryHandler.getClass().getAnnotation(CanalTable.class);
        if (Objects.nonNull(canalTable)) {
            return getCombinationValue(canalTable.destination(), canalTable.schema(), canalTable.table());
        }
        return null;
    }

    public static List<String> getCanalTableNameCombinations(CanalEventHolder eventHolder) {
        OnCanalEvent canalEvent = eventHolder.getEvent();
        if (Objects.nonNull(canalEvent) && Objects.nonNull(canalEvent.eventType()) && canalEvent.eventType().length > 0) {
            return Arrays.stream(canalEvent.eventType())
                    .map(eventType -> getCombinationValue(canalEvent.destination(), canalEvent.schema(), canalEvent.table(), eventType))
                    .distinct().collect(Collectors.toList());
        }
        return null;
    }

    public static String getCombinationValue(String destination, String schema, String table) {
        destination = StringUtils.defaultIfBlank(destination, TableNameEnum.ALL.getDestination());
        schema = StringUtils.defaultIfBlank(schema, TableNameEnum.ALL.getSchema());
        table = StringUtils.defaultIfBlank(table, TableNameEnum.ALL.getTable());
        StringJoiner joiner = new StringJoiner(TableNameEnum.DELIMITER).add(destination).add(schema).add(table);
        return joiner.toString().toLowerCase();
    }

    public static String getCombinationValue(String destination, String schema, String table, CanalEntry.EventType eventType) {
        destination = StringUtils.defaultIfBlank(destination, TableNameEnum.ALL.getDestination());
        schema = StringUtils.defaultIfBlank(schema, TableNameEnum.ALL.getSchema());
        table = StringUtils.defaultIfBlank(table, TableNameEnum.ALL.getTable());
        StringJoiner joiner = new StringJoiner(TableNameEnum.DELIMITER).add(destination).add(schema).add(table).add(eventType.name().toLowerCase());
        return joiner.toString().toLowerCase();
    }

}
