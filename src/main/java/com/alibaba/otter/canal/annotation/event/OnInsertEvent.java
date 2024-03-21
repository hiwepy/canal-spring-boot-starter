package com.alibaba.otter.canal.annotation.event;

import com.alibaba.otter.canal.annotation.OnCanalEvent;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

/**
 * 新增操作监听器 发生insert时 会触发
 *
 * @author lujun
 */

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@OnCanalEvent(eventType = CanalEntry.EventType.INSERT)
public @interface OnInsertEvent {

    /**
     * canal 指令
     * default for all
     *
     */
    @AliasFor(annotation = OnCanalEvent.class)
    String destination() default "";

    /**
     * 数据库实例
     *
     */
    @AliasFor(annotation = OnCanalEvent.class)
    String[] schema() default {};

    /**
     * 监听的表
     * default for all
     *
     */
    @AliasFor(annotation = OnCanalEvent.class)
    String[] table() default {};

}
