package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.model.CanalModel;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CanalModelTest {

    @Test
    void builderCreatesModel() {
        CanalModel model = CanalModel.builder()
                .id(1L)
                .destination("dest")
                .schema("schema")
                .table("table")
                .eventType(CanalEntry.EventType.INSERT)
                .executeTime(1000L)
                .createTime(2000L)
                .build();
        assertThat(model.getId()).isEqualTo(1L);
        assertThat(model.getDestination()).isEqualTo("dest");
        assertThat(model.getSchema()).isEqualTo("schema");
        assertThat(model.getTable()).isEqualTo("table");
        assertThat(model.getEventType()).isEqualTo(CanalEntry.EventType.INSERT);
        assertThat(model.getExecuteTime()).isEqualTo(1000L);
        assertThat(model.getCreateTime()).isEqualTo(2000L);
    }

    @Test
    void settersAndGetters() {
        CanalModel model = new CanalModel();
        model.setId(5L);
        model.setDestination("d");
        model.setSchema("s");
        model.setTable("t");
        model.setEventType(CanalEntry.EventType.UPDATE);
        model.setExecuteTime(100L);
        model.setCreateTime(200L);
        assertThat(model.getId()).isEqualTo(5L);
        assertThat(model.getDestination()).isEqualTo("d");
        assertThat(model.getSchema()).isEqualTo("s");
        assertThat(model.getTable()).isEqualTo("t");
        assertThat(model.getEventType()).isEqualTo(CanalEntry.EventType.UPDATE);
        assertThat(model.getExecuteTime()).isEqualTo(100L);
        assertThat(model.getCreateTime()).isEqualTo(200L);
    }

    @Test
    void toStringContainsFields() {
        CanalModel model = CanalModel.builder()
                .id(1L).schema("s").table("t")
                .eventType(CanalEntry.EventType.INSERT)
                .executeTime(100L).createTime(200L)
                .build();
        String str = model.toString();
        assertThat(str).contains("CanalModel");
        assertThat(str).contains("schema='s'");
        assertThat(str).contains("table='t'");
    }
}
