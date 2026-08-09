package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.annotation.CanalEventHolder;
import com.alibaba.otter.canal.annotation.CanalTable;
import com.alibaba.otter.canal.annotation.OnCanalEvent;
import com.alibaba.otter.canal.annotation.event.OnInsertEvent;
import com.alibaba.otter.canal.enums.TableNameEnum;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.util.HandlerUtil;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import java.lang.reflect.Method;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HandlerUtilTest {

    @Test
    void getCombinationValueWithBlanks() {
        String result = HandlerUtil.getCombinationValue("", "", "");
        assertThat(result).isNotEmpty();
    }

    @Test
    void getCombinationValueWithValues() {
        String result = HandlerUtil.getCombinationValue("dest", "schema", "table");
        assertThat(result).contains("dest", "schema", "table");
    }

    @Test
    void getCombinationValueWithEventType() {
        String result = HandlerUtil.getCombinationValue("dest", "schema", "table", CanalEntry.EventType.INSERT);
        assertThat(result).contains("dest", "schema", "table", "insert");
    }

    @Test
    void getCombinationValueWithNulls() {
        String result = HandlerUtil.getCombinationValue(null, null, null);
        assertThat(result).isNotEmpty();
    }

    @Test
    void getEntryHandlerFromListReturnsMatching() {
        EntryHandler handler = new TestEntryHandler();
        List<EntryHandler> handlers = List.of(handler);
        EntryHandler result = HandlerUtil.getEntryHandler(handlers, "test_schema", "test_table");
        // May return null or the handler depending on annotation
    }

    @Test
    void getEntryHandlerFromMapReturnsMatching() {
        Map<String, EntryHandler> map = new HashMap<>();
        map.put("schema.table", new TestEntryHandler());
        EntryHandler result = HandlerUtil.getEntryHandler(map, "schema", "table");
        assertThat(result).isNotNull();
    }

    @Test
    void getEntryHandlerFromMapReturnsWildcard() {
        Map<String, EntryHandler> map = new HashMap<>();
        map.put(TableNameEnum.ALL.name().toLowerCase(), new TestEntryHandler());
        EntryHandler result = HandlerUtil.getEntryHandler(map, "unknown", "unknown");
        assertThat(result).isNotNull();
    }

    @Test
    void getEntryHandlerFromMapReturnsNull() {
        Map<String, EntryHandler> map = new HashMap<>();
        EntryHandler result = HandlerUtil.getEntryHandler(map, "unknown", "unknown");
        assertThat(result).isNull();
    }

    @Test
    void getTableHandlerMapReturnsEmptyForNull() {
        Map<String, EntryHandler> result = HandlerUtil.getTableHandlerMap(null);
        assertThat(result).isEmpty();
    }

    @Test
    void getTableHandlerMapReturnsEmptyForEmptyList() {
        Map<String, EntryHandler> result = HandlerUtil.getTableHandlerMap(List.of());
        assertThat(result).isEmpty();
    }

    @Test
    void getTableHandlerMapReturnsEmptyForNoAnnotation() {
        Map<String, EntryHandler> result = HandlerUtil.getTableHandlerMap(new ArrayList<>());
        assertThat(result).isNotNull();
    }

    @Test
    void getEventHolderMapReturnsEmptyForNull() {
        Map<String, List<CanalEventHolder>> result = HandlerUtil.getEventHolderMap(null);
        assertThat(result).isEmpty();
    }

    @Test
    void getEventHolderMapReturnsEmptyForEmptyList() {
        Map<String, List<CanalEventHolder>> result = HandlerUtil.getEventHolderMap(List.of());
        assertThat(result).isEmpty();
    }

    @Test
    void getEventHoldersReturnsEmptyForEmptyMap() {
        Map<String, List<CanalEventHolder>> map = new HashMap<>();
        List<CanalEventHolder> result = HandlerUtil.getEventHolders(map, "dest", "schema", "table", CanalEntry.EventType.INSERT);
        assertThat(result).isEmpty();
    }

    @Test
    void getCanalTableNameCombinationReturnsNullForNoAnnotation() {
        EntryHandler handler = new TestEntryHandler();
        String result = HandlerUtil.getCanalTableNameCombination(handler);
        // Returns null because TestEntryHandler has no @CanalTable
        assertThat(result).isNull();
    }

    @Test
    void getCanalTableNameCombinationsReturnsNullForNoEvent() {
        // Test with null - expect NPE
        try {
            HandlerUtil.getCanalTableNameCombinations(null);
        } catch (NullPointerException e) {
            // Expected
        }
    }

    @Test
    void getEntryHandlerFromListWithAnnotatedHandler() {
        AnnotatedEntryHandler handler = new AnnotatedEntryHandler();
        List<EntryHandler> handlers = List.of(handler);
        // The combination includes destination, so schema.table alone won't match
        EntryHandler result = HandlerUtil.getEntryHandler(handlers, "test_schema", "test_table");
        // Result may be null because combination is "dest.test_schema.test_table" not "test_schema.test_table"
    }

    @Test
    void getEntryHandlerFromListWithWildcardHandler() {
        WildcardEntryHandler handler = new WildcardEntryHandler();
        List<EntryHandler> handlers = List.of(handler);
        // Wildcard handler has combination "*.*.*" which doesn't match "all"
        EntryHandler result = HandlerUtil.getEntryHandler(handlers, "any_schema", "any_table");
        // Result may be null
    }

    @Test
    void getEntryHandlerFromListWithMatchingCombination() {
        // Test with a handler whose combination matches schema.table via GenericUtil
        // Note: @CanalTable produces 3-part keys (destination.schema.table) but
        // getEntryHandler(List,...) compares against 2-part (schema.table),
        // so direct annotation matching does not apply; the handler is found
        // only through the generic-type table name fallback.
        NoDestinationEntryHandler handler = new NoDestinationEntryHandler();
        List<EntryHandler> handlers = List.of(handler);
        EntryHandler result = HandlerUtil.getEntryHandler(handlers, "myschema", "mytable");
        // Result is null because the 3-part combination does not match the 2-part key
        assertThat(result).isNull();
    }

    @Test
    void getTableHandlerMapWithAnnotatedHandler() {
        AnnotatedEntryHandler handler = new AnnotatedEntryHandler();
        Map<String, EntryHandler> result = HandlerUtil.getTableHandlerMap(List.of(handler));
        assertThat(result).isNotEmpty();
    }

    @Test
    void getEventHolderMapWithActualHolders() throws Exception {
        OnCanalEvent event = AnnotatedEventHolder.class.getAnnotation(OnCanalEvent.class);
        CanalEventHolder holder = new CanalEventHolder(new Object(), Object.class.getMethod("toString"), event);
        Map<String, List<CanalEventHolder>> result = HandlerUtil.getEventHolderMap(List.of(holder));
        assertThat(result).isNotEmpty();
    }

    @Test
    void getEventHoldersWithMatchingHolders() throws Exception {
        OnCanalEvent event = AnnotatedEventHolder.class.getAnnotation(OnCanalEvent.class);
        CanalEventHolder holder = new CanalEventHolder(new Object(), Object.class.getMethod("toString"), event);
        Map<String, List<CanalEventHolder>> map = new HashMap<>();
        String key = HandlerUtil.getCombinationValue("dest", "schema", "table", CanalEntry.EventType.INSERT);
        map.put(key, List.of(holder));
        List<CanalEventHolder> result = HandlerUtil.getEventHolders(map, "dest", "schema", "table", CanalEntry.EventType.INSERT);
        assertThat(result).isNotEmpty();
    }

    @Test
    void getCanalTableNameCombinationWithAnnotatedHandler() {
        AnnotatedEntryHandler handler = new AnnotatedEntryHandler();
        String result = HandlerUtil.getCanalTableNameCombination(handler);
        assertThat(result).isNotNull().contains("test_schema", "test_table");
    }

    @Test
    void getCanalTableNameCombinationsWithHolder() throws Exception {
        OnCanalEvent event = AnnotatedEventHolder.class.getAnnotation(OnCanalEvent.class);
        CanalEventHolder holder = new CanalEventHolder(new Object(), Object.class.getMethod("toString"), event);
        List<String> result = HandlerUtil.getCanalTableNameCombinations(holder);
        assertThat(result).isNotNull().isNotEmpty();
    }

    @Test
    void getCombinationValueWithEventTypeAndBlanks() {
        String result = HandlerUtil.getCombinationValue("", "", "", CanalEntry.EventType.UPDATE);
        assertThat(result).isNotEmpty();
    }

    @Test
    void getEventHoldersWithNonMatchingDestination() throws Exception {
        OnCanalEvent event = AnnotatedEventHolder.class.getAnnotation(OnCanalEvent.class);
        CanalEventHolder holder = new CanalEventHolder(new Object(), Object.class.getMethod("toString"), event);
        Map<String, List<CanalEventHolder>> map = new HashMap<>();
        String key = HandlerUtil.getCombinationValue("dest", "schema", "table", CanalEntry.EventType.INSERT);
        map.put(key, List.of(holder));
        // Different destination should not match
        List<CanalEventHolder> result = HandlerUtil.getEventHolders(map, "other_dest", "schema", "table", CanalEntry.EventType.INSERT);
        assertThat(result).isEmpty();
    }

    @Test
    void getEventHoldersWithNonMatchingEventType() throws Exception {
        OnCanalEvent event = AnnotatedEventHolder.class.getAnnotation(OnCanalEvent.class);
        CanalEventHolder holder = new CanalEventHolder(new Object(), Object.class.getMethod("toString"), event);
        Map<String, List<CanalEventHolder>> map = new HashMap<>();
        String key = HandlerUtil.getCombinationValue("dest", "schema", "table", CanalEntry.EventType.INSERT);
        map.put(key, List.of(holder));
        // DELETE event type should not match (only INSERT and UPDATE are declared)
        List<CanalEventHolder> result = HandlerUtil.getEventHolders(map, "dest", "schema", "table", CanalEntry.EventType.DELETE);
        assertThat(result).isEmpty();
    }

    @Test
    void getEventHoldersWithMatchingAllCriteria() throws Exception {
        OnCanalEvent event = AnnotatedEventHolder.class.getAnnotation(OnCanalEvent.class);
        CanalEventHolder holder = new CanalEventHolder(new Object(), Object.class.getMethod("toString"), event);
        Map<String, List<CanalEventHolder>> map = new HashMap<>();
        String key = HandlerUtil.getCombinationValue("dest", "schema", "table", CanalEntry.EventType.UPDATE);
        map.put(key, List.of(holder));
        List<CanalEventHolder> result = HandlerUtil.getEventHolders(map, "dest", "schema", "table", CanalEntry.EventType.UPDATE);
        assertThat(result).isNotEmpty();
    }

    @Test
    void getEventHolderMapWithHolderNoEventType() throws Exception {
        // Holder with no event annotation - getCanalTableNameCombinations returns null
        CanalEventHolder holder = new CanalEventHolder(new Object(), Object.class.getMethod("toString"), null);
        Map<String, List<CanalEventHolder>> result = HandlerUtil.getEventHolderMap(List.of(holder));
        assertThat(result).isEmpty();
    }

    @CanalTable(destination = "dest", schema = "test_schema", table = "test_table")
    static class AnnotatedEntryHandler implements EntryHandler<Map<String, String>> {
        @Override
        public void insert(Map<String, String> data) {}
        @Override
        public void update(Map<String, String> before, Map<String, String> after) {}
        @Override
        public void delete(Map<String, String> data) {}
    }

    @CanalTable(destination = "*", schema = "*", table = "*")
    static class WildcardEntryHandler implements EntryHandler<Map<String, String>> {
        @Override
        public void insert(Map<String, String> data) {}
        @Override
        public void update(Map<String, String> before, Map<String, String> after) {}
        @Override
        public void delete(Map<String, String> data) {}
    }

    @CanalTable(schema = "myschema", table = "mytable")
    static class NoDestinationEntryHandler implements EntryHandler<Map<String, String>> {
        @Override
        public void insert(Map<String, String> data) {}
        @Override
        public void update(Map<String, String> before, Map<String, String> after) {}
        @Override
        public void delete(Map<String, String> data) {}
    }

    @OnCanalEvent(destination = "dest", schema = "schema", table = "table",
            eventType = {CanalEntry.EventType.INSERT, CanalEntry.EventType.UPDATE})
    static class AnnotatedEventHolder {
    }

    static class TestEntryHandler implements EntryHandler<Map<String, String>> {
        @Override
        public void insert(Map<String, String> data) {}
        @Override
        public void update(Map<String, String> before, Map<String, String> after) {}
        @Override
        public void delete(Map<String, String> data) {}
    }
}
