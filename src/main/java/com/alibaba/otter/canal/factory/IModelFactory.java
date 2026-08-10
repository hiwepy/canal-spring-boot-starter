package com.alibaba.otter.canal.factory;


import com.alibaba.otter.canal.handler.EntryHandler;

import java.util.Set;

/**
 * Factory contract for materialising Canal row data into an entry model object.
 *
 * @param <T> the raw row data type (e.g. protobuf columns or column maps)
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public interface IModelFactory<T> {


    /**
     * Creates a new entry model instance from the supplied row data.
     *
     * @param entryHandler the handler whose target type drives the conversion
     * @param t            the raw row data
     * @param <R>          the entry model type
     * @return the materialised entry model, or {@code null} if not applicable
     * @throws Exception if conversion fails
     */
    <R> R newInstance(EntryHandler entryHandler, T t) throws Exception;

    /**
     * Creates a new entry model instance restricted to the given updated
     * columns. Used for the before-image of UPDATE events.
     *
     * @param entryHandler  the handler whose target type drives the conversion
     * @param t             the raw row data
     * @param updateColumn  the set of column names that were updated
     * @param <R>           the entry model type
     * @return the materialised entry model, or {@code null} by default
     * @throws Exception if conversion fails
     */
    default <R> R newInstance(EntryHandler entryHandler, T t, Set<String> updateColumn) throws Exception {
        return null;
    }
}
