package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.util.RowDataUtil;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RowDataUtilTest {

    @Test
    void getBeforeValueReturnsColumnValue() {
        CanalEntry.RowData rowData = CanalEntry.RowData.newBuilder()
                .addBeforeColumns(CanalEntry.Column.newBuilder()
                        .setName("id").setValue("1").build())
                .build();
        String result = RowDataUtil.getBeforeValue(rowData, "id");
        assertThat(result).isEqualTo("1");
    }

    @Test
    void getBeforeValueReturnsNullForMissing() {
        CanalEntry.RowData rowData = CanalEntry.RowData.newBuilder().build();
        String result = RowDataUtil.getBeforeValue(rowData, "missing");
        assertThat(result).isNull();
    }

    @Test
    void getAfterValueReturnsColumnValue() {
        CanalEntry.RowData rowData = CanalEntry.RowData.newBuilder()
                .addAfterColumns(CanalEntry.Column.newBuilder()
                        .setName("name").setValue("test").build())
                .build();
        String result = RowDataUtil.getAfterValue(rowData, "name");
        assertThat(result).isEqualTo("test");
    }

    @Test
    void getAfterValueReturnsNullForMissing() {
        CanalEntry.RowData rowData = CanalEntry.RowData.newBuilder().build();
        String result = RowDataUtil.getAfterValue(rowData, "missing");
        assertThat(result).isNull();
    }

    @Test
    void getValueReturnsAfterValue() {
        CanalEntry.RowData rowData = CanalEntry.RowData.newBuilder()
                .addAfterColumns(CanalEntry.Column.newBuilder()
                        .setName("col").setValue("val").build())
                .build();
        String result = RowDataUtil.getValue(rowData, "col");
        assertThat(result).isEqualTo("val");
    }

    @Test
    void getBeforeValueReturnsNullForNullRowData() {
        String result = RowDataUtil.getBeforeValue(null, "col");
        assertThat(result).isNull();
    }

    @Test
    void getAfterValueReturnsNullForNullRowData() {
        String result = RowDataUtil.getAfterValue(null, "col");
        assertThat(result).isNull();
    }

    @Test
    void getValueReturnsNullForNullRowData() {
        String result = RowDataUtil.getValue(null, "col");
        assertThat(result).isNull();
    }

    @Test
    void getValueFallsBackToAfterWhenBeforeIsNull() {
        CanalEntry.RowData rowData = CanalEntry.RowData.newBuilder()
                .addAfterColumns(CanalEntry.Column.newBuilder()
                        .setName("col").setValue("after-val").build())
                .build();
        String result = RowDataUtil.getValue(rowData, "col");
        assertThat(result).isEqualTo("after-val");
    }

    @Test
    void getBeforeValueCaseInsensitive() {
        CanalEntry.RowData rowData = CanalEntry.RowData.newBuilder()
                .addBeforeColumns(CanalEntry.Column.newBuilder()
                        .setName("MyColumn").setValue("val").build())
                .build();
        String result = RowDataUtil.getBeforeValue(rowData, "mycolumn");
        assertThat(result).isEqualTo("val");
    }
}
