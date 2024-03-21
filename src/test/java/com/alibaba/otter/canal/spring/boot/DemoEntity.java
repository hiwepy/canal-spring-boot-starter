package com.alibaba.otter.canal.spring.boot;

import com.baomidou.mybatisplus.annotation.*;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * <p>
 * Demo 表
 * </p>
 *
 */
@Accessors(chain = true)
@Data
@TableName("t_demo")
public class DemoEntity implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * ID
     */
    @TableId(value = "id", type = IdType.INPUT)
    private Long id;

    /**
     * 名称
     */
    @TableField("name")
    private String name;

    /**
     * 图标
     */
    @TableField("icon")
    private String icon;

    /**
     * 显示顺序
     */
    @TableField("order_by")
    private Integer orderBy;

    /**
     * 状态（0:禁用|1:可用）
     */
    @TableField("`status`")
    private Integer status;

    /**
     * 是否删除（0:未删除,1:已删除）
     */
    @TableField("is_deleted")
    @TableLogic
    private Integer isDeleted;

    /**
     * 创建人ID
     */
    @TableField("creator")
    private Long creator;

    /**
     * 创建时间
     */
    @TableField("create_time")
    private LocalDateTime createTime;

    /**
     * 修改人ID
     */
    @TableField("modifyer")
    private Long modifyer;

    /**
     * 修改时间
     */
    @TableField("modify_time")
    private LocalDateTime modifyTime;


}
