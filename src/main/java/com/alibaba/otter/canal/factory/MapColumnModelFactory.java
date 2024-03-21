package com.alibaba.otter.canal.factory;

import com.baomidou.mybatisplus.core.metadata.TableFieldInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.collections.MapUtils;
import org.springframework.beans.BeanUtils;

import java.util.Map;

public class MapColumnModelFactory extends AbstractModelFactory<Map<String, String>> {

    @Override
    <R> R newInstance(Class<R> tableClass, Map<String, String> valueMap) throws Exception {
        R object = BeanUtils.instantiateClass(tableClass);
        // 获取 mybatis-plus 的注解信息
        TableInfo tableInfo = TableInfoHelper.getTableInfo(tableClass);
        // 循环表数据
        for (TableFieldInfo tableFieldInfo:  tableInfo.getFieldList()) {
            // 获取实体对象属性映射字段对应的值
            Object value = MapUtils.getObject(valueMap, tableFieldInfo.getColumn());
            PropertyUtils.setProperty(object, tableFieldInfo.getProperty(), value);
        }
        return object;
    }

}
