package com.alibaba.otter.canal.spring.boot.factory;


import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.spring.boot.enums.TableNameEnum;
import com.alibaba.otter.canal.spring.boot.handler.EntryHandler;
import com.alibaba.otter.canal.spring.boot.utils.GenericUtil;
import com.alibaba.otter.canal.spring.boot.utils.HandlerUtil;
import com.baomidou.mybatisplus.core.metadata.TableFieldInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

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
        Class<R> tableClass = GenericUtil.getTableClass(entryHandler);

        String canalTableName = HandlerUtil.getCanalTableName(entryHandler);
        if (TableNameEnum.ALL.name().toLowerCase().equals(canalTableName)) {
            Map<String, String> map = columns.stream().collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getValue));
            return (R) map;
        }
        Class<R> tableClass = GenericUtil.getTableClass(entryHandler);
        if (tableClass != null) {
            return newInstance(tableClass, columns);
        }
        return (R) map;

        // 获取 mybatis-plus 的注解信息
        TableInfo tableInfo = TableInfoHelper.getTableInfo(tableClass);
        // 循环表数据
        for (TableFieldInfo tableFieldInfo:  tableInfo.getFieldList()) {
            // 获取实体对象属性映射字段对应的值
            Object value = MapUtils.getObject(valueMap, tableFieldInfo.getColumn());
            BeanUtils.setProperty(object, tableFieldInfo.getProperty(), value);
        }
        return object;

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
        TableInfoHelper.getTableInfo()
        Class<R> tableClass = GenericUtil.getTableClass(entryHandler);
        if (tableClass != null) {
            R r = tableClass.newInstance();
            Map<String, String> columnNames = EntryUtil.getFieldName(r.getClass());
            for (CanalEntry.Column column : columns) {
                if (updateColumn.contains(column.getName())) {
                    String fieldName = columnNames.get(column.getName());
                    if (StringUtils.isNotEmpty(fieldName)) {
                        BeanUtils.setProperty(r, fieldName, column.getValue());
                    }
                }
            }
            return r;
        }
        return null;
    }


    @Override
    <R> R newInstance(Class<R> tableClass, List<CanalEntry.Column> columns) throws Exception {
        R object = tableClass.newInstance();
        // 获取 mybatis-plus 的注解信息
        TableInfo tableInfo = TableInfoHelper.getTableInfo(tableClass);
        tableInfo.getFieldList().forEach(field -> {
            String fieldName = field.getProperty();
            CanalEntry.Column column = columns.stream().filter(c -> c.getName().equals(fieldName)).findFirst().orElse(null);
            if (column != null) {
                FieldUtil.setFieldValue(object, fieldName, column.getValue());
            }
        });

        Map<String, String> columnNames = EntryUtil.getFieldName(object.getClass());
        for (CanalEntry.Column column : columns) {
            String fieldName = columnNames.get(column.getName());
            if (StringUtils.isNotEmpty(fieldName)) {
                FieldUtil.setFieldValue(object, fieldName, column.getValue());
            }
        }
        return object;
    }


}
