package com.alibaba.otter.canal.annotation;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.lang.annotation.*;

/**
 * Marks a method as a Canal row-change handler within a
 * {@link CanalEventHandler}-annotated bean.
 * <p>
 * The destination, schema and table attributes default to wildcards so an
 * unannotated method receives events for every table. The {@link #eventType()}
 * attribute has no default and must be supplied explicitly to filter the event
 * types (INSERT, UPDATE, DELETE, ...).
 * </p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OnCanalEvent {

    /**
     * Canal destination (instance) name. Defaults to empty, matching all destinations.
     *
     * @return the destination name
     */
    String destination() default "";

    /**
     * Database (schema) name. Defaults to {@code "*"}, matching all schemas.
     *
     * @return the schema name
     */
    String schema() default "*";

    /**
     * Table name. Defaults to {@code "*"}, matching all tables.
     *
     * @return the table name
     */
    String table() default "*";

    /**
     * Canal event types the method should handle. Must be supplied explicitly.
     *
     * @return the event types to handle
     */
    CanalEntry.EventType[] eventType();

}
