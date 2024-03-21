package com.alibaba.otter.canal.annotation;

import java.lang.annotation.*;

/**
 * @author yang peng
 * @date 2019/3/2710:11
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface CanalTable {

    String value() default "*";

}
