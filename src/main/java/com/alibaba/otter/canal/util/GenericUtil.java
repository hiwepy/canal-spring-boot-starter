package com.alibaba.otter.canal.util;


import com.alibaba.otter.canal.handler.EntryHandler;
import com.alibaba.otter.canal.model.CanalModel;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.baomidou.mybatisplus.core.metadata.TableInfo;
import com.baomidou.mybatisplus.core.metadata.TableInfoHelper;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yang peng
 * @date 2019/3/2910:45
 */
public class GenericUtil {

    private static Map<Class<? extends EntryHandler>, Class> cache = new ConcurrentHashMap<>();

    public static Object[] getInvokeArgs(Method method, CanalModel canalModel, CanalEntry.RowChange rowChange) {
        return Arrays.stream(method.getParameterTypes())
                .map(pClass -> CanalModel.class.isAssignableFrom(pClass) ? canalModel : CanalEntry.RowChange.class.isAssignableFrom(pClass) ? rowChange : null)
                .toArray();
    }

    public static Object[] getInvokeArgs(Method method, CanalModel canalModel, List<Map<String, String>> rowData) {
        return Arrays.stream(method.getParameterTypes())
                .map(pClass -> CanalModel.class.isAssignableFrom(pClass) ? canalModel : List.class.isAssignableFrom(pClass) ? rowData : null)
                .toArray();
    }

    public static String getTableGenericProperties(EntryHandler entryHandler) {
        Class<?> tableClass = getTableClass(entryHandler);
        if (tableClass != null) {
            // 3.2、获取 mybatis-plus 的注解信息
            TableInfo tableInfo = TableInfoHelper.getTableInfo(tableClass);
            if (Objects.nonNull(tableInfo)) {
                return tableInfo.getTableName();
            }
        }
        return null;
    }


    @SuppressWarnings("unchecked")
    public static <T> Class<T> getTableClass(EntryHandler object) {
        // 1、获取处理器的泛型类型
        Class<? extends EntryHandler> handlerClass = object.getClass();
        Class tableClass = cache.get(handlerClass);
        if (tableClass == null) {
            Type[] interfacesTypes = handlerClass.getGenericInterfaces();
            for (Type t : interfacesTypes) {
                Class c = (Class) ((ParameterizedType) t).getRawType();
                if (c.equals(EntryHandler.class)) {
                    tableClass = (Class<T>) ((ParameterizedType) t).getActualTypeArguments()[0];
                    cache.putIfAbsent(handlerClass, tableClass);
                    return tableClass;
                }
            }
        }
        return tableClass;
    }


}
