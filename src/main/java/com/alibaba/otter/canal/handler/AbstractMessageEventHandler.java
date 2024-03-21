package com.alibaba.otter.canal.handler;

import com.alibaba.otter.canal.annotation.OnCanalEvent;
import com.alibaba.otter.canal.context.CanalContext;
import com.alibaba.otter.canal.listener.CanalEventListener;
import com.alibaba.otter.canal.listener.ListenerPoint;
import com.alibaba.otter.canal.model.CanalModel;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.util.GenericUtil;
import com.alibaba.otter.canal.util.HandlerUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Method;
import java.util.*;

/**
 * 抽象消息处理器
 */
@Slf4j
public abstract class AbstractMessageEventHandler implements MessageHandler<Message>, ApplicationContextAware {

    /**
     * 实现接口的 canal 监听器(上：表内容，下：表结构)
     */
    protected final List<CanalEventListener> listeners = new ArrayList<>();
    /**
     * 通过注解方式的 canal 监听器
     */
    protected final List<ListenerPoint> annoListeners = new ArrayList<>();

    @Override
    public  void handleMessage(Message message) {
        // 遍历 entryes，单条解析
        for (CanalEntry.Entry entry : message.getEntries()) {
            // 获取表名
            String tableName = entry.getHeader().getTableName();
            // 获取类型
            CanalEntry.EntryType entryType = entry.getEntryType();
            // 判断当前entryType类型是否为ROWDATA，既当前变化的数据是否行数据
            if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                try {
                    // 设置上下文
                    CanalModel model = CanalModel.Builder.builder()
                            .id(message.getId())
                            .table(tableName)
                            .executeTime(entry.getHeader().getExecuteTime())
                            .database(entry.getHeader().getSchemaName())
                            .build();


                    CanalContext.setModel(model);
                    // 获取序列化后的数据
                    CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());

                    this.distributeByAnnotation(model, rowChange);
                    //this.distributeByImpl(message.getDestination(), entry.getHeader().getSchemaName(), tableName, rowChange);

                    // 获取当前事件的操作类型
                    CanalEntry.EventType eventType = rowChange.getEventType();

                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                } finally {
                    // 移除上下文
                    CanalContext.removeModel();
                }
            } else {
                log.info("当前操作类型为：{}", entryType);
            }
        }
    }

    /**
     * 处理注解方式的 canal 监听器
     * @param model   表名称
     * @param rowChange   数据
     */
    protected void distributeByAnnotation(CanalModel model,
                                          CanalEntry.RowChange rowChange) {

        //对注解的监听器进行事件委托
        if (!CollectionUtils.isEmpty(annoListeners)) {
            annoListeners.forEach(point -> point
                    .getInvokeMap()
                    .entrySet()
                    .stream()
                    .forEach(entry -> {
                        Method method = entry.getKey();
                        method.setAccessible(true);
                        try {
                            Object[] args = GenericUtil.getInvokeArgs(method, model, rowChange);
                            method.invoke(point.getTarget(), args);
                        } catch (Exception e) {
                            log.error("", e);
                            log.error("{}: entrust canal listeners occurs error! error object name :{}, method name :{}",
                                    Thread.currentThread().getName(),
                                    point.getTarget().getClass().getName(), method.getName());
                        }
                    }));
        }
    }

    /**
     * 处理监听信息
     *
     * @param destination 指令
     * @param schemaName  库实例
     * @param tableName   表名
     * @param rowChange   參數
     */
    protected void distributeByImpl(String destination,
                                    String schemaName,
                                    String tableName,
                                    CanalEntry.RowChange rowChange) {
        if (listeners != null) {
            for (CanalEventListener listener : listeners) {
                listener.onEvent(destination, schemaName, tableName, rowChange);
            }
        }
    }

    public void initListeners() {
        log.info("{}: listeners is initializing....", Thread.currentThread().getName());
        //获取监听器
        Collection<CanalEventListener> list = applicationContext.getBeansOfType(CanalEventListener.class).values();
        if (list != null) {
            //若存在目标监听，放入 listenerMap
            listeners.addAll(list);
        }
        //通过注解的方式去监听的话。。
        Map<String, Object> listenerMap = applicationContext.getBeansWithAnnotation(com.alibaba.otter.canal.annotation.CanalEventListener.class);
        //也放入 map
        if (listenerMap != null) {
            for (Object target : listenerMap.values()) {
                //方法获取
                Method[] methods = target.getClass().getDeclaredMethods();
                for (Method method : methods) {
                    OnCanalEvent l = AnnotatedElementUtils.findMergedAnnotation(method, OnCanalEvent.class);
                    if (l != null) {
                        annoListeners.add(new ListenerPoint(target, method, l));
                    }
                }
            }
        }
        //初始化监听结束
        log.info("{}: listener initialized finish.", Thread.currentThread().getName());
        //整个项目上下文都没发现监听器。。。
        if (log.isWarnEnabled() && listeners.isEmpty() && annoListeners.isEmpty()) {
            log.warn("{}: no listener targets in current project ! ", Thread.currentThread().getName());
        }
    }

    protected ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

}
