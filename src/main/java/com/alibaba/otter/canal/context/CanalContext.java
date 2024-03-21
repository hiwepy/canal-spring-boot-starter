package com.alibaba.otter.canal.context;

import com.alibaba.otter.canal.model.CanalModel;
import com.alibaba.ttl.TransmittableThreadLocal;

/**
 * @author yang peng
 * @date 2019/3/2711:17
 */
public class CanalContext {

    private static TransmittableThreadLocal<CanalModel> threadLocal = new TransmittableThreadLocal<>();

    public static CanalModel getModel(){
        return threadLocal.get();
    }


    public static void setModel(CanalModel canalModel){
        threadLocal.set(canalModel);
    }


    public  static void removeModel(){
        threadLocal.remove();
    }
}
