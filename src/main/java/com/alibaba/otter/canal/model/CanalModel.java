package com.alibaba.otter.canal.model;


import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * @author yang peng
 * @date 2019/3/2711:21
 */
@Setter
@Getter
@Builder
public class CanalModel {


    /**
     * 消息id
     */
    private long id;

    /**
     * 库名
     */
    private String destination;

    /**
     * 库名
     */
    private String schema;
    /**
     * 表名
     */
    private String table;
    /**
     * 事件类型
     */
    private CanalEntry.EventType eventType;
    /**
     * binlog executeTime
     */
    private Long executeTime;

    /**
     * dml build timeStamp
     */
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
