package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.model.CanalModel;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.util.GenericUtil;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GenericUtilTest {

    @Test
    void getInvokeArgsWithRowChange() throws Exception {
        Method method = TestHandler.class.getMethod("insert", Map.class);
        CanalModel model = CanalModel.builder().id(1L).build();
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.newBuilder().build();
        Object[] args = GenericUtil.getInvokeArgs(method, model, rowChange, CanalEntry.EventType.INSERT);
        assertThat(args).isNotNull();
    }

    @Test
    void getInvokeArgsWithList() throws Exception {
        Method method = TestHandler.class.getMethod("insert", Map.class);
        CanalModel model = CanalModel.builder().id(1L).build();
        List<Map<String, String>> rowData = List.of(Map.of("k", "v"));
        Object[] args = GenericUtil.getInvokeArgs(method, model, rowData, CanalEntry.EventType.INSERT);
        assertThat(args).isNotNull();
    }

    @Test
    void getInvokeArgsWithUpdateMethod() throws Exception {
        Method method = TestHandler.class.getMethod("update", Map.class, Map.class);
        CanalModel model = CanalModel.builder().id(1L).build();
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.newBuilder().build();
        Object[] args = GenericUtil.getInvokeArgs(method, model, rowChange, CanalEntry.EventType.UPDATE);
        assertThat(args).isNotNull();
    }

    @Test
    void getInvokeArgsWithDeleteMethod() throws Exception {
        Method method = TestHandler.class.getMethod("delete", Map.class);
        CanalModel model = CanalModel.builder().id(1L).build();
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.newBuilder().build();
        Object[] args = GenericUtil.getInvokeArgs(method, model, rowChange, CanalEntry.EventType.DELETE);
        assertThat(args).isNotNull();
    }

    @Test
    void getInvokeArgsWithRowDataListAndEventTypeParam() throws Exception {
        Method method = TestHandler.class.getMethod("insert", Map.class);
        CanalModel model = CanalModel.builder().id(1L).build();
        List<Map<String, String>> rowData = List.of(Map.of("k", "v"));
        Object[] args = GenericUtil.getInvokeArgs(method, model, rowData, CanalEntry.EventType.INSERT);
        assertThat(args).hasSize(1);
    }

    @Test
    void getTableClassReturnsNullForNonGenericHandler() {
        NonGenericHandler handler = new NonGenericHandler();
        Class<?> result = GenericUtil.getTableClass(handler);
        assertThat(result).isNull();
    }

    @Test
    void getTableGenericPropertiesReturnsNullForNonGenericHandler() {
        NonGenericHandler handler = new NonGenericHandler();
        String result = GenericUtil.getTableGenericProperties(handler);
        assertThat(result).isNull();
    }

    @Test
    void getTableClassCachesResult() {
        TestHandler handler1 = new TestHandler();
        TestHandler handler2 = new TestHandler();
        Class<?> result1 = GenericUtil.getTableClass(handler1);
        Class<?> result2 = GenericUtil.getTableClass(handler2);
        assertThat(result1).isSameAs(result2);
    }

    static class TestHandler implements EntryHandler<Map<String, String>> {
        @Override
        public void insert(Map<String, String> data) {}
        @Override
        public void update(Map<String, String> before, Map<String, String> after) {}
        @Override
        public void delete(Map<String, String> data) {}
    }

    static class NonGenericHandler implements EntryHandler {
        @Override
        public void insert(Object data) {}
        @Override
        public void update(Object before, Object after) {}
        @Override
        public void delete(Object data) {}
    }
}
