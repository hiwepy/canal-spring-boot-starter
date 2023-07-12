package com.alibaba.otter.canal.spring.boot.factory;


import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.otter.canal.spring.boot.factory.AbstractModelFactory;
import com.alibaba.otter.canal.spring.boot.utils.EntryUtil;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.core.metadata.TableFieldInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Objects;

public class MapColumnModelFactory extends AbstractModelFactory<Map<String, String>> {

    @Override
    <R> R newInstance(Class<R> clazz, Map<String, String> valueMap) throws Exception {
        R object = c.newInstance();
        Map<String, String> columnNames = EntryUtil.getFieldName(object.getClass());
        for (Map.Entry<String, String> entry : valueMap.entrySet()) {
            String fieldName = columnNames.get(entry.getKey());
            if (StringUtils.isNotEmpty(fieldName)) {
                FieldUtil.setFieldValue(object, fieldName, entry.getValue());
            }
        }

        // 获取 mybatis-plus 的注解信息
        TableInfo tableInfo = TableInfoHelper.getTableInfo(clazz);
        // 循环表数据
        for (TableFieldInfo tableFieldInfo:  tableInfo.getFieldList()) {
            // 获取实体对象属性映射字段对应的值
            MapUtils.getString(valueMap, tableFieldInfo.getColumn());
            Object columnVal = jsonObj.get(tableFieldInfo.getColumn());
            // 添加实体对象属性名称对应的值
            jsonObj.put(tableFieldInfo.getProperty(), columnVal);
            // 查找TableId 注解的属性
            TableId tableId = tableFieldInfo.getField().getAnnotation(TableId.class);
            if (tableId != null) {
                tableIdVal = columnVal;
            }
        }

        // 从binglog数据获取数据内容数组
        JSONArray jsonArray = jsonObject.getJSONArray("data");
        // 循环数据
        for (int i = 0; i < jsonArray.size(); i++) {
            // 获取当前数据
            JSONObject jsonObj = jsonArray.getJSONObject(i);
            Object tableIdVal = null;

            if(Objects.nonNull(tableIdVal)){
                redisTemplate.set(RedisKey.getKeyStr(database, tableName, tableIdVal), jsonObj.to(clazz));
            }
        }

        return object;
    }
}
