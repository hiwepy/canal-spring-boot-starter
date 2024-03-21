package com.alibaba.otter.canal.factory;


import com.alibaba.otter.canal.enums.TableNameEnum;
import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.util.GenericUtil;
import com.alibaba.otter.canal.util.HandlerUtil;
import com.baomidou.mybatisplus.core.metadata.TableFieldInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author yang peng
 * @date 2019/3/2916:16
 */
public class EntryColumnModelFactory extends AbstractModelFactory<List<CanalEntry.Column>> {

    @Override
    public <R> R newInstance(EntryHandler entryHandler, List<CanalEntry.Column> columns) throws Exception {
        String canalTableName = HandlerUtil.getCanalTableName(entryHandler);
        if (TableNameEnum.ALL.name().toLowerCase().equals(canalTableName)) {
            Map<String, String> map = columns.stream().collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getValue));
            return (R) map;
        }
        Class<R> entityClass = GenericUtil.getTableClass(entryHandler);
        if (entityClass != null) {
            return newInstance(entityClass, columns);
        }
        return null;
    }

    @Override
    public <R> R newInstance(EntryHandler entryHandler, List<CanalEntry.Column> columns, Set<String> updateColumn) throws Exception {
        String canalTableName = HandlerUtil.getCanalTableName(entryHandler);
        if (TableNameEnum.ALL.name().toLowerCase().equals(canalTableName)) {
            Map<String, String> map = columns.stream().filter(column -> updateColumn.contains(column.getName()))
                    .collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getValue));
            return (R) map;
        }
        Class<R> tableClass = GenericUtil.getTableClass(entryHandler);
        if (tableClass != null) {
            // 获取 mybatis-plus 的注解信息
            TableInfo tableInfo = TableInfoHelper.getTableInfo(tableClass);
            // 创建实体对象
            R object = BeanUtils.instantiateClass(tableClass);
            for (CanalEntry.Column column : columns) {
                if (updateColumn.contains(column.getName())) {
                    // 循环表数据
                    for (TableFieldInfo tableFieldInfo:  tableInfo.getFieldList()) {
                        String fieldName = tableFieldInfo.getProperty();
                        // 获取实体对象属性映射字段对应的值
                        if (StringUtils.equals(tableFieldInfo.getColumn(), column.getName())) {
                            PropertyUtils.setProperty(object, fieldName, column.getValue());
                            break;
                        }
                    }
                }
            }
            return object;
        }
        return null;
    }


    @Override
    <R> R newInstance(Class<R> rtClass, List<CanalEntry.Column> columns) throws Exception {
        // 如果列为空，返回null
        if(CollectionUtils.isEmpty(columns)){
            return null;
        }
        // 创建实体对象
        R object = BeanUtils.instantiateClass(rtClass);
        // 获取 mybatis-plus 的注解信息
        TableInfo tableInfo = TableInfoHelper.getTableInfo(rtClass);
        // 循环表数据
        for (TableFieldInfo tableFieldInfo:  tableInfo.getFieldList()) {
            String fieldName = tableFieldInfo.getProperty();
            for (CanalEntry.Column column : columns) {
                // 获取实体对象属性映射字段对应的值
                if (StringUtils.equals(tableFieldInfo.getColumn(), column.getName())) {
                    PropertyUtils.setProperty(object, fieldName, column.getValue());
                    break;
                }
            }
        }
        return object;
    }

}
