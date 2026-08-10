package com.alibaba.otter.canal.util;


import com.alibaba.otter.canal.annotation.CanalEventHolder;
import com.alibaba.otter.canal.annotation.CanalTable;
import com.alibaba.otter.canal.annotation.OnCanalEvent;
import com.alibaba.otter.canal.enums.TableNameEnum;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Registry helpers for resolving the {@link EntryHandler} or
 * {@link CanalEventHolder} that should handle a given Canal row-change event.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public class HandlerUtil {

    /** Cache of annotation filters keyed by destination/schema/table/event. */
    protected static Map<String, Predicate<CanalEventHolder>> eventPredicateMap = new ConcurrentHashMap<>();

    /**
     * Resolves the programmatic {@link EntryHandler} for the given schema/table
     * from the supplied list, honouring the {@link TableNameEnum#ALL} wildcard
     * as a fallback.
     *
     * @param entryHandlers the registered handlers
     * @param schemaName    the database schema name
     * @param tableName     the table name
     * @return the matching handler, the wildcard handler, or {@code null}
     */
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


    /**
     * Indexes programmatic {@link EntryHandler} instances by their bound
     * destination/schema/table combination.
     *
     * @param entryHandlers the registered handlers
     * @return a map keyed by lower-cased destination.schema.table combinations
     */
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
     * Indexes annotation-based {@link CanalEventHolder} instances, grouped by
     * the destination/schema/table/eventType combination derived from each
     * holder's {@link OnCanalEvent} annotation.
     *
     * @param eventHolders the registered annotation-based holders
     * @return a map keyed by lower-cased destination.schema.table.event combinations
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

    /**
     * Resolves the annotation-based {@link CanalEventHolder} list for the given
     * destination/schema/table/event, applying the cached annotation filter.
     *
     * @param map         the event-holder index
     * @param destination the Canal destination
     * @param schemaName  the database schema name
     * @param tableName   the table name
     * @param eventType   the Canal event type
     * @return the matching holders (never {@code null})
     */
    public static List<CanalEventHolder> getEventHolders(Map<String, List<CanalEventHolder>> map,
                                                        String destination,
                                                        String schemaName,
                                                        String tableName,
                                                        CanalEntry.EventType eventType) {
        // Build the destination/schema/table/event key.
        String key = getCombinationValue(destination, schemaName, tableName, eventType);
        // Resolve (or create) the matching annotation filter.
        Predicate<CanalEventHolder> predicate =  eventPredicateMap.computeIfAbsent(key, k -> getAnnotationFilter(destination, schemaName, tableName, eventType));
        // Return the filtered holders.
        return map.getOrDefault(key, Collections.emptyList()).stream().filter(predicate).collect(Collectors.toList());
    }

    /**
     * Resolves the programmatic {@link EntryHandler} for the given schema/table
     * from the index, falling back to the {@link TableNameEnum#ALL} wildcard.
     *
     * @param map        the handler index
     * @param schemaName the database schema name
     * @param tableName  the table name
     * @return the matching handler, the wildcard handler, or {@code null}
     */
    public static EntryHandler getEntryHandler(Map<String, EntryHandler> map, String schemaName, String tableName) {
        StringJoiner joiner = new StringJoiner(".").add(schemaName).add(tableName);
        EntryHandler entryHandler = map.get(joiner.toString().toLowerCase());
        if (entryHandler == null) {
            return map.get(TableNameEnum.ALL.name().toLowerCase());
        }
        return entryHandler;
    }

    /**
     * Builds a predicate that matches annotation-based holders whose
     * destination, schema, table and event type all align with the supplied
     * values (treating blank/wildcard attributes as match-all).
     *
     * @param destination the Canal destination
     * @param schemaName  the database schema name
     * @param tableName   the table name
     * @param eventType   the Canal event type
     * @return a predicate that matches compatible holders
     */
    protected static Predicate<CanalEventHolder> getAnnotationFilter(String destination,
                                                                     String schemaName,
                                                                     String tableName,
                                                                     CanalEntry.EventType eventType) {

        // Match destination: blank annotation value or null event destination means all.
        Predicate<CanalEventHolder> df = holder -> StringUtils.isEmpty(holder.getEvent().destination())
                || holder.getEvent().destination().equals(destination) || destination == null;

        // Match schema name (case-insensitive).
        Predicate<CanalEventHolder> sf = holder -> StringUtils.isNotBlank(holder.getEvent().schema())
                && holder.getEvent().schema().equalsIgnoreCase(schemaName);

        // Match table name: blank means all; wildcard matches everything.
        Predicate<CanalEventHolder> tf = holder -> StringUtils.isNotBlank(holder.getEvent().table())
                && ( holder.getEvent().table().equalsIgnoreCase(tableName) || holder.getEvent().table().equals(TableNameEnum.ALL.getTable()) );

        // Match event type.
        Predicate<CanalEventHolder> ef = holder -> holder.getEvent().eventType().length > 0 && Arrays.stream(holder.getEvent().eventType()).anyMatch(ev -> ev == eventType) ;

        return df.and(sf).and(tf).and(ef);
    }

    /**
     * Resolves the destination/schema/table combination bound to an
     * {@link EntryHandler} via its {@link CanalTable} annotation.
     *
     * @param entryHandler the handler to inspect
     * @return the lower-cased combination, or {@code null} when unannotated
     */
    public static String getCanalTableNameCombination(EntryHandler entryHandler) {
        CanalTable canalTable = entryHandler.getClass().getAnnotation(CanalTable.class);
        if (Objects.nonNull(canalTable)) {
            return getCombinationValue(canalTable.destination(), canalTable.schema(), canalTable.table());
        }
        return null;
    }

    /**
     * Resolves the list of destination/schema/table/event combinations covered
     * by a holder's {@link OnCanalEvent} annotation (one per declared event type).
     *
     * @param eventHolder the holder to inspect
     * @return the lower-cased combinations, or {@code null} when no event type is declared
     */
    public static List<String> getCanalTableNameCombinations(CanalEventHolder eventHolder) {
        OnCanalEvent canalEvent = eventHolder.getEvent();
        if (Objects.nonNull(canalEvent) && Objects.nonNull(canalEvent.eventType()) && canalEvent.eventType().length > 0) {
            return Arrays.stream(canalEvent.eventType())
                    .map(eventType -> getCombinationValue(canalEvent.destination(), canalEvent.schema(), canalEvent.table(), eventType))
                    .distinct().collect(Collectors.toList());
        }
        return null;
    }

    /**
     * Builds a lower-cased {@code destination.schema.table} key, defaulting blank
     * segments to the {@link TableNameEnum#ALL} wildcards.
     *
     * @param destination the destination segment
     * @param schema      the schema segment
     * @param table       the table segment
     * @return the lower-cased combination key
     */
    public static String getCombinationValue(String destination, String schema, String table) {
        destination = StringUtils.defaultIfBlank(destination, TableNameEnum.ALL.getDestination());
        schema = StringUtils.defaultIfBlank(schema, TableNameEnum.ALL.getSchema());
        table = StringUtils.defaultIfBlank(table, TableNameEnum.ALL.getTable());
        StringJoiner joiner = new StringJoiner(TableNameEnum.DELIMITER).add(destination).add(schema).add(table);
        return joiner.toString().toLowerCase();
    }

    /**
     * Builds a lower-cased {@code destination.schema.table.event} key, defaulting
     * blank segments to the {@link TableNameEnum#ALL} wildcards.
     *
     * @param destination the destination segment
     * @param schema      the schema segment
     * @param table       the table segment
     * @param eventType   the event type segment
     * @return the lower-cased combination key
     */
    public static String getCombinationValue(String destination, String schema, String table, CanalEntry.EventType eventType) {
        destination = StringUtils.defaultIfBlank(destination, TableNameEnum.ALL.getDestination());
        schema = StringUtils.defaultIfBlank(schema, TableNameEnum.ALL.getSchema());
        table = StringUtils.defaultIfBlank(table, TableNameEnum.ALL.getTable());
        StringJoiner joiner = new StringJoiner(TableNameEnum.DELIMITER).add(destination).add(schema).add(table).add(eventType.name().toLowerCase());
        return joiner.toString().toLowerCase();
    }

}
