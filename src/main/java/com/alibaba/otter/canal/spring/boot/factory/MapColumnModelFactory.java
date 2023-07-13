package com.alibaba.otter.canal.spring.boot.factory;

import com.baomidou.mybatisplus.core.metadata.TableFieldInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections.MapUtils;

import java.util.Map;

public class MapColumnModelFactory extends AbstractModelFactory<Map<String, String>> {

    @Override
    <R> R newInstance(Class<R> clazz, Map<String, String> valueMap) throws Exception {
        R object =  org.springframework.beans.BeanUtils.instantiateClass(clazz);
        // 获取 mybatis-plus 的注解信息
        TableInfo tableInfo = TableInfoHelper.getTableInfo(clazz);
        // 循环表数据
        for (TableFieldInfo tableFieldInfo:  tableInfo.getFieldList()) {
            // 获取实体对象属性映射字段对应的值
            Object value = MapUtils.getObject(valueMap, tableFieldInfo.getColumn());
            BeanUtils.setProperty(object, tableFieldInfo.getProperty(), value);
        }
        return object;
    }

}
