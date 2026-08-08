package com.alibaba.otter.canal.annotation;

import org.springframework.core.annotation.AliasFor;
import org.springframework.stereotype.Component;

import java.lang.annotation.*;

/**
 * Marks a class as a Canal event handler and registers it as a Spring
 * {@link Component}.
 * <p>
 * Methods within the annotated class can be further annotated with
 * {@link OnCanalEvent} to receive specific row-change events. The Canal message
 * handlers scan the application context for beans annotated with
 * {@code @CanalEventHandler} on startup.
 * </p>
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface CanalEventHandler {

    /**
     * Alias for the {@link Component#value() component name}.
     *
     * @return the bean name
     */
    @AliasFor(annotation = Component.class)
    String value() default "";

}
