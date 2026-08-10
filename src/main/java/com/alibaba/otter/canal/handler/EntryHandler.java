package com.alibaba.otter.canal.handler;

/**
 * Callback interface for handling table row change events of a specific model type.
 * <p>
 * Implementations are typically annotated with {@code @CanalTable} and registered
 * as Spring beans. The appropriate method is invoked by the row data handler when
 * an insert, update or delete event is received for the bound table.
 * </p>
 *
 * @param <R> the entry model type bound to this handler
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public interface EntryHandler<R> {



    /**
     * Callback invoked when a row is inserted.
     *
     * @param t the inserted entry model
     */
    default void insert(R t) {

    }


    /**
     * Callback invoked when a row is updated.
     *
     * @param before the entry model before the update
     * @param after  the entry model after the update
     */
    default void update(R before, R after) {

    }


    /**
     * Callback invoked when a row is deleted.
     *
     * @param t the deleted entry model
     */
    default void delete(R t) {

    }
}
