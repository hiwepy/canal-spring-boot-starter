package com.alibaba.otter.canal.factory;


import com.alibaba.otter.canal.enums.TableNameEnum;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.util.GenericUtil;
import com.alibaba.otter.canal.util.HandlerUtil;

/**
 * Base {@link IModelFactory} implementation that resolves the target model type
 * from the {@link EntryHandler} generic signature (or {@link TableNameEnum#ALL}
 * wildcard) and delegates the actual conversion to
 * {@link #newInstance(Class, Object)}.
 *
 * @param <T> the raw row data type
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public abstract class AbstractModelFactory<T> implements IModelFactory<T> {

    @Override
    /**
     * <p>New instance.</p>
     * @param entryHandler
     * @param t
     * @return the result
     */
    public <R> R newInstance(EntryHandler entryHandler, T t) throws Exception {
        String canalTableName = HandlerUtil.getCanalTableNameCombination(entryHandler);
        if (TableNameEnum.ALL.name().toLowerCase().equals(canalTableName)) {
            return (R) t;
        }
        Class<R> tableClass = GenericUtil.getTableClass(entryHandler);
        if (tableClass != null) {
            return newInstance(tableClass, t);
        }
        return null;
    }

    /**
     * Creates a new instance of the given target class from the supplied row data.
     *
     * @param tableClass the target entry model class
     * @param t          the raw row data
     * @param <R>        the entry model type
     * @return the materialised entry model
     * @throws Exception if conversion fails
     */
    abstract <R> R newInstance(Class<R> tableClass, T t) throws Exception;
}
