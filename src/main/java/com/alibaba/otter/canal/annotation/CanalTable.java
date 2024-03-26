package com.alibaba.otter.canal.annotation;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface CanalTable {

    /**
     * canal 指令
     * default for all
     *
     */
    String destination() default "";

    /**
     * 数据库实例
     *
     */
    String schema() default "*";

    /**
     * 监听的表
     * default for all
     *
     */
    String table() default "*";

}
