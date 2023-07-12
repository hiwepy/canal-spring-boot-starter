package com.alibaba.otter.canal.spring.boot.factory;


import com.alibaba.otter.canal.spring.boot.handler.EntryHandler;

import java.util.Set;

/**
 * @author yang peng
 * @date 2019/3/2916:45
 */
public interface IModelFactory<T> {


    <R> R newInstance(EntryHandler entryHandler, T t) throws Exception;

    default <R> R newInstance(EntryHandler entryHandler, T t, Set<String> updateColumn) throws Exception {
        return null;
    }
}
