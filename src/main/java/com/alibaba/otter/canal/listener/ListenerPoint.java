package com.alibaba.otter.canal.listener;


import com.alibaba.otter.canal.annotation.OnCanalEvent;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 监听 canal 操作
 *
 * @author lujun
 */
public class ListenerPoint {
    /**
     * 目标
     */
    private final Object target;

    /**
     * 监听的方法和节点
     */
    private final Map<Method, OnCanalEvent> invokeMap = new ConcurrentHashMap<>();

    /**
     * 构造方法，设置目标，方法以及注解类型
     *
     */
    public ListenerPoint(Object target, Method method, OnCanalEvent anno) {
        this.target = target;
        this.invokeMap.put(method, anno);
    }

    /**
     * 返回目标类
     *
     */
    public Object getTarget() {
        return target;
    }

    /**
     * 获取监听的操作方法和节点
     *
     */
    public Map<Method, OnCanalEvent> getInvokeMap() {
        return invokeMap;
    }

}
