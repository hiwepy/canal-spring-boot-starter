package com.alibaba.otter.canal.annotation;

import java.lang.annotation.*;

/**
 * Binds an {@link com.alibaba.otter.canal.handler.EntryHandler} to a specific
 * Canal destination, database schema and/or table.
 * <p>
 * All attributes default to wildcards ({@code "*"} for schema/table and empty
 * for destination) so an unannotated handler matches every table.
 * </p>
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface CanalTable {

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

}
