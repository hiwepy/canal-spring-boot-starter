package com.alibaba.otter.canal.annotation.content;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.tianyin.canal.core.annotation.OnCanalEvent;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

/**
 * 删除操作监听器 当删除数据库的记录时 添加该注解的方法会被调用
 *
 * @author lujun
 */

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@OnCanalEvent(eventType = CanalEntry.EventType.DELETE)
public @interface OnDeleteEvent {

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
