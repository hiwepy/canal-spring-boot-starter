package com.alibaba.otter.canal.annotation;

import org.springframework.core.annotation.AliasFor;
import org.springframework.stereotype.Component;

import java.lang.annotation.*;

/**
 * Canal 处理器注解，继承 @Component
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface CanalEventHandler {

    @AliasFor(annotation = Component.class)
    String value() default "";

}
