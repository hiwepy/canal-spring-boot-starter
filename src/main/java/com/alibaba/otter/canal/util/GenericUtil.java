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
 * Reflection helpers for resolving {@link EntryHandler} generic types and
 * building reflective method invocation argument arrays.
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public class GenericUtil {

    /** Cache of resolved entry model classes keyed by handler class. */
    private static Map<Class<? extends EntryHandler>, Class> cache = new ConcurrentHashMap<>();

    /**
     * Builds the argument array for invoking an annotation-based handler method,
     * matching each declared parameter type to the supplied model, row change or
     * event type.
     *
     * @param method    the method to invoke
     * @param model     the Canal context model
     * @param rowChange the row change to pass
     * @param eventType the Canal event type
     * @return the resolved arguments in declaration order
     */
    public static Object[] getInvokeArgs(Method method, CanalModel model, CanalEntry.RowChange rowChange, CanalEntry.EventType eventType) {
        return Arrays.stream(method.getParameterTypes()).map(pClass -> {
                    if(CanalModel.class.isAssignableFrom(pClass)){
                        return model;
                    }
                    if(CanalEntry.RowChange.class.isAssignableFrom(pClass)) {
                        return rowChange;
                    }
                    if(CanalEntry.EventType.class.isAssignableFrom(pClass)) {
                        return eventType;
                    }
                    return null;
                })
                .toArray();
    }

    /**
     * Builds the argument array for invoking an annotation-based handler method,
     * matching each declared parameter type to the supplied model, row data or
     * event type.
     *
     * @param method    the method to invoke
     * @param model     the Canal context model
     * @param rowData   the flat row data (column maps) to pass
     * @param eventType the Canal event type
     * @return the resolved arguments in declaration order
     */
    public static Object[] getInvokeArgs(Method method, CanalModel model, List<Map<String, String>> rowData, CanalEntry.EventType eventType) {
        return Arrays.stream(method.getParameterTypes()).map(pClass -> {
                if(CanalModel.class.isAssignableFrom(pClass)){
                    return model;
                }
                if(List.class.isAssignableFrom(pClass)) {
                    return rowData;
                }
                if(CanalEntry.EventType.class.isAssignableFrom(pClass)) {
                    return eventType;
                }
                return null;
            }).toArray();
    }

    /**
     * Resolves the MyBatis-Plus table name for the entry model bound to the
     * given handler.
     *
     * @param entryHandler the handler to inspect
     * @return the resolved table name, or {@code null} if not available
     */
    public static String getTableGenericProperties(EntryHandler entryHandler) {
        Class<?> tableClass = getTableClass(entryHandler);
        if (tableClass != null) {
            // Resolve the MyBatis-Plus table metadata.
            TableInfo tableInfo = TableInfoHelper.getTableInfo(tableClass);
            if (Objects.nonNull(tableInfo)) {
                return tableInfo.getTableName();
            }
        }
        return null;
    }


    /**
     * Resolves the entry model class bound to the given handler by inspecting
     * the {@link EntryHandler} generic signature. Results are cached per handler
     * class.
     *
     * @param object the handler to inspect
     * @param <T>    the entry model type
     * @return the resolved entry model class, or {@code null} if not found
     */
    @SuppressWarnings("unchecked")
    /** @return return the table class. */
    public static <T> Class<T> getTableClass(EntryHandler object) {
        // Resolve the handler's generic type argument.
        Class<? extends EntryHandler> handlerClass = object.getClass();
        Class tableClass = cache.get(handlerClass);
        if (tableClass == null) {
            Type[] interfacesTypes = handlerClass.getGenericInterfaces();
            for (Type t : interfacesTypes) {
                if (!(t instanceof ParameterizedType)) {
                    continue;
                }
                ParameterizedType pt = (ParameterizedType) t;
                Class c = (Class) pt.getRawType();
                if (c.equals(EntryHandler.class)) {
                    Type typeArg = pt.getActualTypeArguments()[0];
                    if (typeArg instanceof Class) {
                        tableClass = (Class<T>) typeArg;
                    } else if (typeArg instanceof ParameterizedType) {
                        tableClass = (Class<T>) ((ParameterizedType) typeArg).getRawType();
                    } else {
                        continue;
                    }
                    cache.putIfAbsent(handlerClass, tableClass);
                    return tableClass;
                }
            }
        }
        return tableClass;
    }


}
