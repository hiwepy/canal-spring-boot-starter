package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class CanalPropertiesTest {

    @Test
    void defaultValues() {
        CanalProperties props = new CanalProperties();
        assertThat(props.getMode()).isEqualTo(CanalProperties.ClientMode.simple);
        assertThat(props.getAsync()).isNull();
        assertThat(props.getFilter()).isEmpty();
        assertThat(props.getBatchSize()).isEqualTo(1000);
        assertThat(props.getTimeout()).isEqualTo(-1L);
        assertThat(props.getUnit()).isEqualTo(TimeUnit.SECONDS);
        assertThat(props.getSubscribeTypes()).contains(CanalEntry.EntryType.ROWDATA);
    }

    @Test
    void setAndGetMode() {
        CanalProperties props = new CanalProperties();
        props.setMode(CanalProperties.ClientMode.kafka);
        assertThat(props.getMode()).isEqualTo(CanalProperties.ClientMode.kafka);
    }

    @Test
    void setAndGetAsync() {
        CanalProperties props = new CanalProperties();
        props.setAsync(true);
        assertThat(props.getAsync()).isTrue();
    }

    @Test
    void setAndGetFilter() {
        CanalProperties props = new CanalProperties();
        props.setFilter("test");
        assertThat(props.getFilter()).isEqualTo("test");
    }

    @Test
    void setAndGetBatchSize() {
        CanalProperties props = new CanalProperties();
        props.setBatchSize(500);
        assertThat(props.getBatchSize()).isEqualTo(500);
    }

    @Test
    void setAndGetTimeout() {
        CanalProperties props = new CanalProperties();
        props.setTimeout(5000L);
        assertThat(props.getTimeout()).isEqualTo(5000L);
    }

    @Test
    void setAndGetUnit() {
        CanalProperties props = new CanalProperties();
        props.setUnit(TimeUnit.MILLISECONDS);
        assertThat(props.getUnit()).isEqualTo(TimeUnit.MILLISECONDS);
    }

    @Test
    void prefixConstant() {
        assertThat(CanalProperties.PREFIX).isEqualTo("canal");
    }

    @Test
    void clientModeValues() {
        assertThat(CanalProperties.ClientMode.values()).hasSize(6);
    }

    @Test
    void setAndGetSubscribeTypes() {
        CanalProperties props = new CanalProperties();
        props.setSubscribeTypes(java.util.List.of(CanalEntry.EntryType.ROWDATA, CanalEntry.EntryType.TRANSACTIONBEGIN));
        assertThat(props.getSubscribeTypes()).hasSize(2);
    }
}
