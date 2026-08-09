package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.enums.TableNameEnum;
import com.alibaba.otter.canal.handler.CanalThreadUncaughtExceptionHandler;
import com.alibaba.otter.canal.annotation.CanalEventHandler;
import com.alibaba.otter.canal.annotation.CanalTable;
import com.alibaba.otter.canal.annotation.OnCanalEvent;
import com.alibaba.otter.canal.annotation.event.*;
import com.alibaba.otter.canal.context.CanalContext;
import com.alibaba.otter.canal.model.CanalModel;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EnumsAndAnnotationsTest {

    @Test
    void tableNameEnumValues() {
        assertThat(TableNameEnum.ALL).isNotNull();
        assertThat(TableNameEnum.ALL.getDestination()).isEqualTo("*");
        assertThat(TableNameEnum.ALL.getSchema()).isEqualTo("*");
        assertThat(TableNameEnum.ALL.getTable()).isEqualTo("*");
        assertThat(TableNameEnum.DELIMITER.toString()).isEqualTo(".");
        assertThat(TableNameEnum.ALL.toString()).isEqualTo("*.*");
    }

    @Test
    void uncaughtExceptionHandlerLogsError() {
        CanalThreadUncaughtExceptionHandler handler = new CanalThreadUncaughtExceptionHandler();
        handler.uncaughtException(Thread.currentThread(), new RuntimeException("test"));
        // Just verify no exception thrown
    }

    @Test
    void canalContextRoundTrip() {
        CanalModel model = CanalModel.builder()
                .id(1L).schema("s").table("t")
                .eventType(CanalEntry.EventType.INSERT)
                .build();
        CanalContext.setModel(model);
        assertThat(CanalContext.getModel()).isEqualTo(model);
        CanalContext.removeModel();
        assertThat(CanalContext.getModel()).isNull();
    }
}
