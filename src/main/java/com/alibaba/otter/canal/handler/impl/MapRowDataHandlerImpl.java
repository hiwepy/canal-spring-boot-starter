package com.alibaba.otter.canal.handler.impl;


import com.alibaba.otter.canal.factory.IModelFactory;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.handler.RowDataHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@link RowDataHandler} for flat Canal messages ({@code List<Map<String, String>>}).
 * <p>
 * Uses a {@link IModelFactory} to materialise column maps into the target entry
 * model and then invokes the matching {@link EntryHandler} callback
 * ({@code insert}, {@code update} or {@code delete}) based on the event type.
 * For UPDATE events the list is expected to contain the after-image at index 0
 * and the before-image at index 1.
 * </p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public class MapRowDataHandlerImpl implements RowDataHandler<List<Map<String, String>>> {

    /** Factory used to materialise column maps into entry model instances. */
    private IModelFactory<Map<String,String>> modelFactory;

    /**
     * @param modelFactory the model factory used to create entry model instances
     */
    public MapRowDataHandlerImpl(IModelFactory<Map<String, String>> modelFactory) {
        this.modelFactory = modelFactory;
    }

    /**
     * Converts the column maps into a model object and dispatches it to the
     * matching {@link EntryHandler} callback based on {@code eventType}.
     *
     * @param list         the row data as column maps; for UPDATE the after-image is at index 0 and before-image at index 1
     * @param entryHandler the handler to dispatch the converted model to
     * @param eventType    the Canal event type (INSERT, UPDATE, DELETE, ...)
     * @param <R>          the entry model type
     * @throws Exception if model creation or dispatch fails
     */
    @Override
    public <R> void handlerRowData(List<Map<String, String>> list, EntryHandler<R> entryHandler, CanalEntry.EventType eventType) throws Exception{
        if (Objects.isNull(list) || Objects.isNull(entryHandler) || Objects.isNull(eventType)) {
            return;
        }
        switch (eventType) {
            case INSERT:
                R entry  = modelFactory.newInstance(entryHandler, list.get(0));
                entryHandler.insert(entry);
                break;
            case UPDATE:
                R before = modelFactory.newInstance(entryHandler, list.get(1));
                R after = modelFactory.newInstance(entryHandler, list.get(0));
                entryHandler.update(before, after);
                break;
            case DELETE:
                R o = modelFactory.newInstance(entryHandler, list.get(0));
                entryHandler.delete(o);
                break;
            default:
                break;
        }
    }
}
