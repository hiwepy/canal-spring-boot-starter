package com.alibaba.otter.canal.handler.impl;


import com.alibaba.otter.canal.factory.IModelFactory;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.handler.RowDataHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link RowDataHandler} for protobuf Canal entries ({@link CanalEntry.RowData}).
 * <p>
 * Uses a {@link IModelFactory} to materialise row columns into the target entry
 * model and then invokes the matching {@link EntryHandler} callback
 * ({@code insert}, {@code update} or {@code delete}) based on the event type.
 * For UPDATE events only the columns flagged as updated are propagated to the
 * before-image.
 * </p>
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */
public class RowDataHandlerImpl implements RowDataHandler<CanalEntry.RowData> {


    /** Factory used to materialise row columns into entry model instances. */
    private IModelFactory<List<CanalEntry.Column>> modelFactory;


    /**
     * @param modelFactory the model factory used to create entry model instances
     */
    public RowDataHandlerImpl(IModelFactory modelFactory) {
        this.modelFactory = modelFactory;
    }

    /**
     * Converts the row data into a model object and dispatches it to the
     * matching {@link EntryHandler} callback based on {@code eventType}.
     *
     * @param rowData      the protobuf row data
     * @param entryHandler the handler to dispatch the converted model to
     * @param eventType    the Canal event type (INSERT, UPDATE, DELETE, ...)
     * @param <R>          the entry model type
     * @throws Exception if model creation or dispatch fails
     */
    @Override
    public <R> void handlerRowData(CanalEntry.RowData rowData, EntryHandler<R> entryHandler, CanalEntry.EventType eventType) throws Exception {
        if (Objects.isNull(rowData) || Objects.isNull(entryHandler) || Objects.isNull(eventType)) {
            return;
        }
        switch (eventType) {
            case INSERT:
                R object = modelFactory.newInstance(entryHandler, rowData.getAfterColumnsList());
                entryHandler.insert(object);
                break;
            case UPDATE:
                Set<String> updateColumnSet = rowData.getAfterColumnsList().stream().filter(CanalEntry.Column::getUpdated)
                        .map(CanalEntry.Column::getName).collect(Collectors.toSet());
                R before = modelFactory.newInstance(entryHandler, rowData.getBeforeColumnsList(),updateColumnSet);
                R after = modelFactory.newInstance(entryHandler, rowData.getAfterColumnsList());
                entryHandler.update(before, after);
                break;
            case DELETE:
                R o = modelFactory.newInstance(entryHandler, rowData.getBeforeColumnsList());
                entryHandler.delete(o);
                break;
            default:
                break;
        }
    }
}
