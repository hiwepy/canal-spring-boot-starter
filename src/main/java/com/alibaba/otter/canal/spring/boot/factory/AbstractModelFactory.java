package com.alibaba.otter.canal.spring.boot.factory;


import com.alibaba.otter.canal.spring.boot.enums.TableNameEnum;
import com.alibaba.otter.canal.spring.boot.handler.EntryHandler;
import com.alibaba.otter.canal.spring.boot.utils.GenericUtil;
import com.alibaba.otter.canal.spring.boot.utils.HandlerUtil;

public abstract class AbstractModelFactory<T> implements IModelFactory<T> {

    @Override
    public <R> R newInstance(EntryHandler entryHandler, T t) throws Exception {
        String canalTableName = HandlerUtil.getCanalTableName(entryHandler);
        if (TableNameEnum.ALL.name().toLowerCase().equals(canalTableName)) {
            return (R) t;
        }
        Class<R> tableClass = GenericUtil.getTableClass(entryHandler);
        if (tableClass != null) {
            return newInstance(tableClass, t);
        }
        return null;
    }


    abstract <R> R newInstance(Class<R> tableClass, T t) throws Exception;
}
