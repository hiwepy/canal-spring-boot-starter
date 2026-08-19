package com.alibaba.otter.canal.annotation.event;

import com.alibaba.otter.canal.annotation.OnCanalEvent;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

/**
 * Listens for Canal {@code DINDEX} (index drop) events.
 * <p>
 * A composed alias for {@link OnCanalEvent} that fixes the event type to
 * {@link CanalEntry.EventType#DINDEX}. Apply it to a method of a
 * {@link com.alibaba.otter.canal.annotation.CanalEventHandler}-annotated bean to
 * receive index-drop events for the matching destination/schema/table.
 * </p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@OnCanalEvent(eventType = CanalEntry.EventType.DINDEX)
/**
 * <p>Auto-configuration for OnDropIndexEvent.</p>
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public @interface OnDropIndexEvent {
    /**
     * Canal destination (instance) name. Defaults to empty, matching all destinations.
     *
     * @return the destination name
     */
    @AliasFor(annotation = OnCanalEvent.class)
    String destination() default "";

    /**
     * Database (schema) name.
     *
     * @return the schema name
     */
    @AliasFor(annotation = OnCanalEvent.class)
    String schema();

    /**
     * Table name. Defaults to wildcard, matching all tables.
     *
     * @return the table name
     */
    @AliasFor(annotation = OnCanalEvent.class)
    String table();
}
