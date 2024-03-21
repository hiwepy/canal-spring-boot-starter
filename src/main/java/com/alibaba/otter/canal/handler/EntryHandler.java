package com.alibaba.otter.canal.handler;

/**
 * @author yang peng
 * @date 2019/3/2915:46
 */
public interface EntryHandler<R> {



    default void insert(R t) {

    }


    default void update(R before, R after) {

    }


    default void delete(R t) {

    }
}
