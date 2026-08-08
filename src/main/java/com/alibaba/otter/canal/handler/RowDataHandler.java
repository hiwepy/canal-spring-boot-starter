package com.alibaba.otter.canal.handler;

import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * Strategy for transforming raw Canal row data into a model object and
 * dispatching it to an {@link EntryHandler}.
 *
 * @param <T> the raw row data type (e.g. {@code CanalEntry.RowData} or a list of column maps)
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */
public interface RowDataHandler<T> {

    /**
     * Converts the given row data into a model object and dispatches it to the
     * matching callback on {@code entryHandler} based on {@code eventType}.
     *
     * @param t             the raw row data
     * @param entryHandler  the handler to dispatch the converted model to
     * @param eventType     the Canal event type (INSERT, UPDATE, DELETE, ...)
     * @param <R>           the entry model type
     * @throws Exception if conversion or dispatch fails
     */
    <R> void handlerRowData(T t, EntryHandler<R> entryHandler, CanalEntry.EventType eventType) throws Exception;

}
