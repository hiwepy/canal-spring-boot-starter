package com.alibaba.otter.canal.annotation;


import com.alibaba.otter.canal.protocol.CanalEntry;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * Holds the metadata for a single {@link OnCanalEvent}-annotated method,
 * allowing the Canal message handlers to invoke it reflectively when a matching
 * row-change event is received.
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */
public class CanalEventHolder {

    /** The target bean instance owning the annotated method. */
    private Object target;
    /** The annotated method to invoke. */
    private Method method;
    /** The {@link OnCanalEvent} annotation driving the binding. */
    private OnCanalEvent event;

    /**
     * Creates a new holder wrapping the target bean, method and annotation.
     *
     * @param target the bean instance owning the method
     * @param method the annotated method to invoke
     * @param event  the {@link OnCanalEvent} annotation
     */
    public CanalEventHolder(Object target, Method method, OnCanalEvent event) {
        this.target = target;
        this.method = method;
        this.event = event;
    }

    /**
     * @return the target bean instance
     */
    public Object getTarget() {
        return target;
    }

    /**
     * @return the annotated method
     */
    public Method getMethod() {
        return method;
    }

    /**
     * @return the {@link OnCanalEvent} annotation
     */
    public OnCanalEvent getEvent() {
        return event;
    }

    /**
     * Determines whether this holder should handle the given event type.
     *
     * @param eventType the Canal event type to test
     * @return {@code true} if the annotation declares no event types, declares a
     *         matching type, or the supplied type is {@code null}
     */
    public boolean isMatch(CanalEntry.EventType eventType) {
        return this.getEvent().eventType().length == 0 || Arrays.stream(this.getEvent().eventType()).anyMatch(ev -> ev == eventType) || eventType == null;
    }

}
