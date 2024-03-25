package com.alibaba.otter.canal.annotation;


import com.alibaba.otter.canal.protocol.CanalEntry;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * 监听 canal 操作
 *
 * @author lujun
 */
public class CanalEventHolder {

    /**
     * 目标
     */
    private Object target;
    /**
     * 监听的方法
     */
    private Method method;
    /**
     * 监听的事件
     */
    private OnCanalEvent event;

    /**
     * 构造方法，设置目标，方法以及注解类型
     *
     */
    public CanalEventHolder(Object target, Method method, OnCanalEvent event) {
        this.target = target;
        this.method = method;
        this.event = event;
    }

    /**
     * 返回目标类
     *
     */
    public Object getTarget() {
        return target;
    }

    /**
     * 返回方法
     *
     */
    public Method getMethod() {
        return method;
    }

    /**
     * 返回注解类型
     *
     */
    public OnCanalEvent getEvent() {
        return event;
    }

    public boolean isMatch(CanalEntry.EventType eventType) {
        return this.getEvent().eventType().length == 0 || Arrays.stream(this.getEvent().eventType()).anyMatch(ev -> ev == eventType) || eventType == null;
    }

}
