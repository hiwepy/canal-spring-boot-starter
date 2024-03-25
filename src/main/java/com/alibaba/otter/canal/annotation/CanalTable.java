package com.alibaba.otter.canal.annotation;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface CanalTable {

    /**
     * 数据库实例
     */
    String schema() default "*";

    /**
     * 监听的表
     * default for all
     *
     */
    String table() default "*";

}
