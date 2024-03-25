package com.alibaba.otter.canal.annotation.event;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.annotation.OnCanalEvent;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

/**
 * 表结构发生变化，新增时，先判断数据库实例是否存在，不存在则创建
 *
 * @author lujun
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@OnCanalEvent(eventType = CanalEntry.EventType.CREATE)
public @interface OnCreateTableEvent {

    /**
     * canal 指令
     * default for all
     */
    @AliasFor(annotation = OnCanalEvent.class)
    String destination() default "";

    /**
     * 数据库实例
     */
    @AliasFor(annotation = OnCanalEvent.class)
    String schema();
}
