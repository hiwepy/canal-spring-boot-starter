package com.alibaba.otter.canal.spring.boot;

import com.alibaba.otter.canal.handler.EntryHandler;

public class DefEntryHandler implements EntryHandler<DemoEntity> {

    @Override
    public void insert(DemoEntity demoEntity) {
        EntryHandler.super.insert(demoEntity);
    }



}
