package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.util.CanalUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CanalUtilsTest {

    @Test
    void printSummaryWithEmptyMessage() {
        Message message = new Message(1L, false, new ArrayList<>());
        CanalUtils.printSummary(message, 1L, 0);
    }

    @Test
    void printSummaryWithEntries() {
        CanalEntry.Entry entry = CanalEntry.Entry.newBuilder()
                .setEntryType(CanalEntry.EntryType.ROWDATA)
                .setHeader(CanalEntry.Header.newBuilder()
                        .setSchemaName("test")
                        .setTableName("table")
                        .setEventType(CanalEntry.EventType.INSERT)
                        .build())
                .build();
        Message message = new Message(1L, false, List.of(entry));
        CanalUtils.printSummary(message, 1L, 1);
    }

    @Test
    void printEntryWithEmptyList() {
        CanalUtils.printEntry(new ArrayList<>());
    }

    @Test
    void printEntryWithRowdataEntry() {
        CanalEntry.Entry entry = CanalEntry.Entry.newBuilder()
                .setEntryType(CanalEntry.EntryType.ROWDATA)
                .setHeader(CanalEntry.Header.newBuilder()
                        .setSchemaName("test")
                        .setTableName("table")
                        .setEventType(CanalEntry.EventType.INSERT)
                        .setExecuteTime(System.currentTimeMillis())
                        .build())
                .setStoreValue(CanalEntry.RowChange.newBuilder()
                        .setEventType(CanalEntry.EventType.INSERT)
                        .addRowDatas(CanalEntry.RowData.newBuilder()
                                .addAfterColumns(CanalEntry.Column.newBuilder()
                                        .setName("id").setValue("1").setIsKey(true).build())
                                .addAfterColumns(CanalEntry.Column.newBuilder()
                                        .setName("name").setValue("test").build())
                                .build())
                        .build().toByteString())
                .build();
        CanalUtils.printEntry(List.of(entry));
    }

    @Test
    void printEntryWithTransactionEntries() {
        CanalEntry.Entry beginEntry = CanalEntry.Entry.newBuilder()
                .setEntryType(CanalEntry.EntryType.TRANSACTIONBEGIN)
                .setHeader(CanalEntry.Header.newBuilder().build())
                .build();
        CanalEntry.Entry endEntry = CanalEntry.Entry.newBuilder()
                .setEntryType(CanalEntry.EntryType.TRANSACTIONEND)
                .setHeader(CanalEntry.Header.newBuilder().build())
                .build();
        CanalUtils.printEntry(List.of(beginEntry, endEntry));
    }

    @Test
    void printColumn() {
        List<CanalEntry.Column> columns = List.of(
                CanalEntry.Column.newBuilder().setName("id").setValue("1").setIsKey(true).build(),
                CanalEntry.Column.newBuilder().setName("name").setValue("test").setUpdated(true).build(),
                CanalEntry.Column.newBuilder().setName("empty").setValue("").setIsNull(true).build()
        );
        CanalUtils.printColumn(columns);
    }

    @Test
    void printColumnEmpty() {
        CanalUtils.printColumn(new ArrayList<>());
    }

    @Test
    void buildPositionForDump() {
        CanalEntry.Entry entry = CanalEntry.Entry.newBuilder()
                .setEntryType(CanalEntry.EntryType.ROWDATA)
                .setHeader(CanalEntry.Header.newBuilder()
                        .setLogfileName("mysql-bin.000001")
                        .setLogfileOffset(100)
                        .setServerId(1L)
                        .build())
                .build();
        String position = CanalUtils.buildPositionForDump(entry);
        assertThat(position).isNotNull();
    }

    @Test
    void getCurrentGtid() {
        CanalEntry.Header header = CanalEntry.Header.newBuilder()
                .setGtid("12345678-1234-1234-1234-123456789012:1-100")
                .build();
        String gtid = CanalUtils.getCurrentGtid(header);
        assertThat(gtid).isNotNull();
    }

    @Test
    void getCurrentGtidEmpty() {
        CanalEntry.Header header = CanalEntry.Header.newBuilder().build();
        String gtid = CanalUtils.getCurrentGtid(header);
        assertThat(gtid).isEmpty();
    }

    @Test
    void getCurrentGtidSn() {
        CanalEntry.Header header = CanalEntry.Header.newBuilder()
                .setGtid("12345678-1234-1234-1234-123456789012:1-100")
                .build();
        String sn = CanalUtils.getCurrentGtidSn(header);
        assertThat(sn).isNotNull();
    }

    @Test
    void getCurrentGtidLct() {
        CanalEntry.Header header = CanalEntry.Header.newBuilder()
                .setGtid("12345678-1234-1234-1234-123456789012:1-100")
                .build();
        String lct = CanalUtils.getCurrentGtidLct(header);
        assertThat(lct).isNotNull();
    }

    @Test
    void printXAInfo() {
        CanalUtils.printXAInfo(new ArrayList<>());
    }

    @Test
    void printXAInfoWithNull() {
        CanalUtils.printXAInfo(null);
    }

    @Test
    void printXAInfoWithXAData() {
        List<CanalEntry.Pair> pairs = List.of(
                CanalEntry.Pair.newBuilder().setKey("XA_TYPE").setValue("XA_START").build(),
                CanalEntry.Pair.newBuilder().setKey("XA_XID").setValue("xid-123").build()
        );
        CanalUtils.printXAInfo(pairs);
    }

    @Test
    void printXAInfoWithPartialXAData() {
        List<CanalEntry.Pair> pairs = List.of(
                CanalEntry.Pair.newBuilder().setKey("XA_TYPE").setValue("XA_START").build()
        );
        CanalUtils.printXAInfo(pairs);
    }

    @Test
    void buildPositionForDumpWithGtid() {
        CanalEntry.Entry entry = CanalEntry.Entry.newBuilder()
                .setEntryType(CanalEntry.EntryType.ROWDATA)
                .setHeader(CanalEntry.Header.newBuilder()
                        .setLogfileName("mysql-bin.000001")
                        .setLogfileOffset(100)
                        .setServerId(1L)
                        .setExecuteTime(System.currentTimeMillis())
                        .setGtid("12345678-1234-1234-1234-123456789012:1-100")
                        .build())
                .build();
        String position = CanalUtils.buildPositionForDump(entry);
        assertThat(position).contains("gtid");
    }

    @Test
    void getCurrentGtidWithProps() {
        CanalEntry.Header header = CanalEntry.Header.newBuilder()
                .addProps(CanalEntry.Pair.newBuilder().setKey("curtGtid").setValue("gtid-value").build())
                .build();
        String gtid = CanalUtils.getCurrentGtid(header);
        assertThat(gtid).isEqualTo("gtid-value");
    }

    @Test
    void getCurrentGtidSnWithProps() {
        CanalEntry.Header header = CanalEntry.Header.newBuilder()
                .addProps(CanalEntry.Pair.newBuilder().setKey("curtGtidSn").setValue("sn-value").build())
                .build();
        String sn = CanalUtils.getCurrentGtidSn(header);
        assertThat(sn).isEqualTo("sn-value");
    }

    @Test
    void getCurrentGtidLctWithProps() {
        CanalEntry.Header header = CanalEntry.Header.newBuilder()
                .addProps(CanalEntry.Pair.newBuilder().setKey("curtGtidLct").setValue("lct-value").build())
                .build();
        String lct = CanalUtils.getCurrentGtidLct(header);
        assertThat(lct).isEqualTo("lct-value");
    }

    @Test
    void printEntryWithDdlEntry() {
        CanalEntry.Entry entry = CanalEntry.Entry.newBuilder()
                .setEntryType(CanalEntry.EntryType.ROWDATA)
                .setHeader(CanalEntry.Header.newBuilder()
                        .setSchemaName("test")
                        .setTableName("table")
                        .setEventType(CanalEntry.EventType.QUERY)
                        .setExecuteTime(System.currentTimeMillis())
                        .build())
                .setStoreValue(CanalEntry.RowChange.newBuilder()
                        .setEventType(CanalEntry.EventType.QUERY)
                        .setIsDdl(true)
                        .setSql("ALTER TABLE test ADD COLUMN col1 VARCHAR(100)")
                        .build().toByteString())
                .build();
        CanalUtils.printEntry(List.of(entry));
    }

    @Test
    void printEntryWithDeleteEntry() {
        CanalEntry.Entry entry = CanalEntry.Entry.newBuilder()
                .setEntryType(CanalEntry.EntryType.ROWDATA)
                .setHeader(CanalEntry.Header.newBuilder()
                        .setSchemaName("test")
                        .setTableName("table")
                        .setEventType(CanalEntry.EventType.DELETE)
                        .setExecuteTime(System.currentTimeMillis())
                        .build())
                .setStoreValue(CanalEntry.RowChange.newBuilder()
                        .setEventType(CanalEntry.EventType.DELETE)
                        .addRowDatas(CanalEntry.RowData.newBuilder()
                                .addBeforeColumns(CanalEntry.Column.newBuilder()
                                        .setName("id").setValue("1").setIsKey(true).build())
                                .build())
                        .build().toByteString())
                .build();
        CanalUtils.printEntry(List.of(entry));
    }

    @Test
    void printEntryWithUpdateEntry() {
        CanalEntry.Entry entry = CanalEntry.Entry.newBuilder()
                .setEntryType(CanalEntry.EntryType.ROWDATA)
                .setHeader(CanalEntry.Header.newBuilder()
                        .setSchemaName("test")
                        .setTableName("table")
                        .setEventType(CanalEntry.EventType.UPDATE)
                        .setExecuteTime(System.currentTimeMillis())
                        .build())
                .setStoreValue(CanalEntry.RowChange.newBuilder()
                        .setEventType(CanalEntry.EventType.UPDATE)
                        .addRowDatas(CanalEntry.RowData.newBuilder()
                                .addAfterColumns(CanalEntry.Column.newBuilder()
                                        .setName("id").setValue("1").setIsKey(true).setUpdated(true).build())
                                .build())
                        .build().toByteString())
                .build();
        CanalUtils.printEntry(List.of(entry));
    }

    @Test
    void printColumnWithBlobType() {
        List<CanalEntry.Column> columns = List.of(
                CanalEntry.Column.newBuilder()
                        .setName("blob_col").setValue("binary-data")
                        .setMysqlType("BLOB").build(),
                CanalEntry.Column.newBuilder()
                        .setName("binary_col").setValue("binary-data")
                        .setMysqlType("BINARY").build()
        );
        CanalUtils.printColumn(columns);
    }

    @Test
    void printEntryWithTransactionBeginAndEnd() {
        CanalEntry.TransactionBegin begin = CanalEntry.TransactionBegin.newBuilder()
                .setThreadId(123L)
                .build();
        CanalEntry.TransactionEnd end = CanalEntry.TransactionEnd.newBuilder()
                .setTransactionId("456")
                .build();
        CanalEntry.Entry beginEntry = CanalEntry.Entry.newBuilder()
                .setEntryType(CanalEntry.EntryType.TRANSACTIONBEGIN)
                .setHeader(CanalEntry.Header.newBuilder()
                        .setExecuteTime(System.currentTimeMillis())
                        .build())
                .setStoreValue(begin.toByteString())
                .build();
        CanalEntry.Entry endEntry = CanalEntry.Entry.newBuilder()
                .setEntryType(CanalEntry.EntryType.TRANSACTIONEND)
                .setHeader(CanalEntry.Header.newBuilder()
                        .setExecuteTime(System.currentTimeMillis())
                        .build())
                .setStoreValue(end.toByteString())
                .build();
        CanalUtils.printEntry(List.of(beginEntry, endEntry));
    }

    @Test
    void printEntryWithDdlQuery() {
        CanalEntry.Entry entry = CanalEntry.Entry.newBuilder()
                .setEntryType(CanalEntry.EntryType.ROWDATA)
                .setHeader(CanalEntry.Header.newBuilder()
                        .setSchemaName("test")
                        .setTableName("table")
                        .setEventType(CanalEntry.EventType.QUERY)
                        .setExecuteTime(System.currentTimeMillis())
                        .build())
                .setStoreValue(CanalEntry.RowChange.newBuilder()
                        .setEventType(CanalEntry.EventType.QUERY)
                        .setIsDdl(true)
                        .setSql("CREATE TABLE test (id INT)")
                        .addRowDatas(CanalEntry.RowData.newBuilder().build())
                        .build().toByteString())
                .build();
        CanalUtils.printEntry(List.of(entry));
    }

    @Test
    void printSummaryWithMultipleEntries() {
        CanalEntry.Entry entry1 = CanalEntry.Entry.newBuilder()
                .setEntryType(CanalEntry.EntryType.ROWDATA)
                .setHeader(CanalEntry.Header.newBuilder()
                        .setLogfileName("mysql-bin.000001")
                        .setLogfileOffset(100)
                        .setExecuteTime(System.currentTimeMillis())
                        .setGtid("gtid-1")
                        .build())
                .build();
        CanalEntry.Entry entry2 = CanalEntry.Entry.newBuilder()
                .setEntryType(CanalEntry.EntryType.ROWDATA)
                .setHeader(CanalEntry.Header.newBuilder()
                        .setLogfileName("mysql-bin.000001")
                        .setLogfileOffset(200)
                        .setExecuteTime(System.currentTimeMillis())
                        .setGtid("gtid-2")
                        .build())
                .build();
        Message message = new Message(1L, false, List.of(entry1, entry2));
        CanalUtils.printSummary(message, 1L, 2);
    }
}
