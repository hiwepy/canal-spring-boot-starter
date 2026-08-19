package com.alibaba.otter.canal.annotation.event;

import com.alibaba.otter.canal.annotation.OnCanalEvent;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

/**
 * Listens for Canal {@code ERASE} (table drop) events.
 * <p>
 * A composed alias for {@link OnCanalEvent} that fixes the event type to
 * {@link CanalEntry.EventType#ERASE}. Apply it to a method of a
 * {@link com.alibaba.otter.canal.annotation.CanalEventHandler}-annotated bean to
 * receive table-drop events for the matching destination/schema.
 * </p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@OnCanalEvent(eventType = CanalEntry.EventType.ERASE)
/**
 * <p>Auto-configuration for OnDropTableEvent.</p>
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public @interface OnDropTableEvent {
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
}
