package com.alibaba.otter.canal.model;


import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

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
@Setter
@Getter
@Builder
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

}
