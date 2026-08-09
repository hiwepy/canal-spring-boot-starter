package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.context.CanalContext;
import com.alibaba.otter.canal.model.CanalModel;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CanalContextTest {

    @AfterEach
    void tearDown() {
        CanalContext.removeModel();
    }

    @Test
    void setAndGetModel() {
        CanalModel model = CanalModel.builder()
                .id(1L).schema("s").table("t")
                .eventType(CanalEntry.EventType.INSERT)
                .build();
        CanalContext.setModel(model);
        assertThat(CanalContext.getModel()).isEqualTo(model);
    }

    @Test
    void removeModel() {
        CanalModel model = CanalModel.builder().id(1L).build();
        CanalContext.setModel(model);
        CanalContext.removeModel();
        assertThat(CanalContext.getModel()).isNull();
    }

    @Test
    void getModelReturnsNullByDefault() {
        assertThat(CanalContext.getModel()).isNull();
    }
}
