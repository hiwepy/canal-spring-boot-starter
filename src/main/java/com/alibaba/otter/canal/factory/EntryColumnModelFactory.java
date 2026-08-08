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
 * {@link IModelFactory} that materialises protobuf Canal columns
 * ({@link CanalEntry.Column}) into entry model instances.
 * <p>
 * When the handler is bound to the {@link TableNameEnum#ALL} wildcard the
 * columns are returned as a name/value map. Otherwise the target entity class
 * is resolved from the handler's generic signature and populated using its
 * MyBatis-Plus {@link TableInfo} column-to-property mapping.
 * </p>
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */
public class EntryColumnModelFactory extends AbstractModelFactory<List<CanalEntry.Column>> {

    @Override
    public <R> R newInstance(EntryHandler entryHandler, List<CanalEntry.Column> columns) throws Exception {
        String canalTableName = HandlerUtil.getCanalTableNameCombination(entryHandler);
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
        String canalTableName = HandlerUtil.getCanalTableNameCombination(entryHandler);
        if (TableNameEnum.ALL.name().toLowerCase().equals(canalTableName)) {
            Map<String, String> map = columns.stream().filter(column -> updateColumn.contains(column.getName()))
                    .collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getValue));
            return (R) map;
        }
        Class<R> tableClass = GenericUtil.getTableClass(entryHandler);
        if (tableClass != null) {
            // Resolve the MyBatis-Plus table metadata.
            TableInfo tableInfo = TableInfoHelper.getTableInfo(tableClass);
            // Instantiate the entity.
            R object = BeanUtils.instantiateClass(tableClass);
            for (CanalEntry.Column column : columns) {
                if (updateColumn.contains(column.getName())) {
                    // Iterate over mapped fields.
                    for (TableFieldInfo tableFieldInfo:  tableInfo.getFieldList()) {
                        String fieldName = tableFieldInfo.getProperty();
                        // Map the column value onto the matching property.
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
        // Return null when the column list is empty.
        if(CollectionUtils.isEmpty(columns)){
            return null;
        }
        // Instantiate the entity.
        R object = BeanUtils.instantiateClass(rtClass);
        // Resolve the MyBatis-Plus table metadata.
        TableInfo tableInfo = TableInfoHelper.getTableInfo(rtClass);
        // Iterate over mapped fields.
        for (TableFieldInfo tableFieldInfo:  tableInfo.getFieldList()) {
            String fieldName = tableFieldInfo.getProperty();
            for (CanalEntry.Column column : columns) {
                // Map the column value onto the matching property.
                if (StringUtils.equals(tableFieldInfo.getColumn(), column.getName())) {
                    PropertyUtils.setProperty(object, fieldName, column.getValue());
                    break;
                }
            }
        }
        return object;
    }

}
