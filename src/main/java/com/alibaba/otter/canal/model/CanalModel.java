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
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
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

    /**
     * <p>Builder.</p>
     * @return the result
     */
    public static CanalModelBuilder builder() {
        return new CanalModelBuilder();
    }

    /** @return return the id. */
    public long getId() { return id; }
    /** @param id set the id. */
    public void setId(long id) { this.id = id; }
    /** @return return the destination. */
    public String getDestination() { return destination; }
    /** @param destination set the destination. */
    public void setDestination(String destination) { this.destination = destination; }
    /** @return return the schema. */
    public String getSchema() { return schema; }
    /** @param schema set the schema. */
    public void setSchema(String schema) { this.schema = schema; }
    /** @return return the table. */
    public String getTable() { return table; }
    /** @param table set the table. */
    public void setTable(String table) { this.table = table; }
    public CanalEntry.EventType getEventType() { return eventType; }
    /** @param eventType set the event type. */
    public void setEventType(CanalEntry.EventType eventType) { this.eventType = eventType; }
    /** @return return the execute time. */
    public Long getExecuteTime() { return executeTime; }
    /** @param executeTime set the execute time. */
    public void setExecuteTime(Long executeTime) { this.executeTime = executeTime; }
    /** @return return the create time. */
    public Long getCreateTime() { return createTime; }
    /** @param createTime set the create time. */
    public void setCreateTime(Long createTime) { this.createTime = createTime; }

    @Override
    /**
     * <p>To string.</p>
     * @return the result
     */
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

    /**
     * <p>Auto-configuration for CanalModelBuilder.</p>
     * @author <a href="https://github.com/loong10k">Loong Wan</a>
     * @since 1.0.0
     */
    public static class CanalModelBuilder {
        private long id;
        private String destination;
        private String schema;
        private String table;
        private CanalEntry.EventType eventType;
        private Long executeTime;
        private Long createTime;

        CanalModelBuilder() {}

        /**
         * <p>Id.</p>
         * @param id
         * @return the result
         */
        public CanalModelBuilder id(long id) { this.id = id; return this; }
        /**
         * <p>Destination.</p>
         * @param destination
         * @return the result
         */
        public CanalModelBuilder destination(String destination) { this.destination = destination; return this; }
        /**
         * <p>Schema.</p>
         * @param schema
         * @return the result
         */
        public CanalModelBuilder schema(String schema) { this.schema = schema; return this; }
        /**
         * <p>Table.</p>
         * @param table
         * @return the result
         */
        public CanalModelBuilder table(String table) { this.table = table; return this; }
        /**
         * <p>Event type.</p>
         * @param eventType
         * @return the result
         */
        public CanalModelBuilder eventType(CanalEntry.EventType eventType) { this.eventType = eventType; return this; }
        /**
         * <p>Execute time.</p>
         * @param executeTime
         * @return the result
         */
        public CanalModelBuilder executeTime(Long executeTime) { this.executeTime = executeTime; return this; }
        /**
         * <p>Create time.</p>
         * @param createTime
         * @return the result
         */
        public CanalModelBuilder createTime(Long createTime) { this.createTime = createTime; return this; }

        /**
         * <p>Build.</p>
         * @return the result
         */
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
