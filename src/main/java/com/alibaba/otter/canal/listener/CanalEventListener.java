package com.alibaba.otter.canal.listener;

import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 **
 * canal 的事件接口层（表数据改变）
 *
 * @author lujun
 */
@FunctionalInterface
public interface CanalEventListener {


    /**
     * 处理事件
     *
     * @param destination 指令
     * @param schemaName  库实例
     * @param tableName   表名
     * @param rowChange   詳細參數
     */
    void onEvent(String destination, String schemaName, String tableName, CanalEntry.RowChange rowChange);

}
