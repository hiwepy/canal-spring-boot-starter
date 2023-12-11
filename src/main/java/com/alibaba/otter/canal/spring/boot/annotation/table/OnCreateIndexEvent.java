package com.alibaba.otter.canal.spring.boot.annotation.table;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.spring.boot.annotation.OnCanalEvent;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

/**
 * 创建索引的操作
 *
 * @author lujun
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@OnCanalEvent(eventType = CanalEntry.EventType.CINDEX)
public @interface OnCreateIndexEvent {
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
    String[] schema() default {};

    /**
     * 监听的表
     * default for all
     */
    @AliasFor(annotation = OnCanalEvent.class)
    String[] table() default {};
}
