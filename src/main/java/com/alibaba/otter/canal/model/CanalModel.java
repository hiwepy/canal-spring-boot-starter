package com.alibaba.otter.canal.model;


import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * Immutable snapshot of the metadata for a Canal row-change event.
 * <p>
 * Bound to {@link CanalContext} for the duration of a handler invocation so
 * that business code can read the originating schema, table, event type and
 * timing without altering method signatures.
 * </p>
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */
public class CanalModel {

    /** Canal batch/message id. */
    private long id;
    /** Canal destination name. */
    private String destination;
    /** Database (schema) name the change originated from. */
    private String schema;
    /** Table name the change originated from. */
    private String table;
    /** Canal event type (INSERT, UPDATE, DELETE, ...). */
    private CanalEntry.EventType eventType;
    /** Binlog execute time in milliseconds. */
    private Long executeTime;
    /** DML build timestamp in milliseconds. */
    private Long createTime;

    public static CanalModelBuilder builder() {
        return new CanalModelBuilder();
    }

    public long getId() { return id; }
    public void setId(long id) { this.id = id; }
    public String getDestination() { return destination; }
    public void setDestination(String destination) { this.destination = destination; }
    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }
    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }
    public CanalEntry.EventType getEventType() { return eventType; }
    public void setEventType(CanalEntry.EventType eventType) { this.eventType = eventType; }
    public Long getExecuteTime() { return executeTime; }
    public void setExecuteTime(Long executeTime) { this.executeTime = executeTime; }
    public Long getCreateTime() { return createTime; }
    public void setCreateTime(Long createTime) { this.createTime = createTime; }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CanalModel{");
        sb.append("id=").append(id);
        sb.append(", schema='").append(schema).append('\'');
        sb.append(", table='").append(table).append('\'');
        sb.append(", eventType='").append(eventType).append('\'');
        sb.append(", executeTime=").append(executeTime);
        sb.append(", createTime=").append(createTime);
        sb.append('}');
        return sb.toString();
    }

    public static class CanalModelBuilder {
        private long id;
        private String destination;
        private String schema;
        private String table;
        private CanalEntry.EventType eventType;
        private Long executeTime;
        private Long createTime;

        CanalModelBuilder() {}

        public CanalModelBuilder id(long id) { this.id = id; return this; }
        public CanalModelBuilder destination(String destination) { this.destination = destination; return this; }
        public CanalModelBuilder schema(String schema) { this.schema = schema; return this; }
        public CanalModelBuilder table(String table) { this.table = table; return this; }
        public CanalModelBuilder eventType(CanalEntry.EventType eventType) { this.eventType = eventType; return this; }
        public CanalModelBuilder executeTime(Long executeTime) { this.executeTime = executeTime; return this; }
        public CanalModelBuilder createTime(Long createTime) { this.createTime = createTime; return this; }

        public CanalModel build() {
            CanalModel model = new CanalModel();
            model.setId(id);
            model.setDestination(destination);
            model.setSchema(schema);
            model.setTable(table);
            model.setEventType(eventType);
            model.setExecuteTime(executeTime);
            model.setCreateTime(createTime);
            return model;
        }
    }

}
