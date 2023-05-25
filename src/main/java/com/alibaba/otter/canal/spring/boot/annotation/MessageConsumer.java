package com.alibaba.otter.canal.spring.boot.annotation;


import java.lang.annotation.*;

/**
 * 消费者监听注解
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface MessageConsumer {

    /**
     * topic
     *
     * @return
     */
    String topic();

    /**
     * tag
     *
     * @return
     */
    String tag() default "*";

}
