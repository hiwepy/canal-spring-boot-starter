package com.alibaba.otter.canal.context;

import com.alibaba.otter.canal.model.CanalModel;
import com.alibaba.ttl.TransmittableThreadLocal;

/**
 * Thread-local holder for the {@link CanalModel} associated with the row change
 * currently being processed.
 * <p>
 * Uses {@link TransmittableThreadLocal} so that the context propagates across
 * thread-pool hand-offs (e.g. when the async message handler dispatches work to
 * the Canal task executor). Handlers can read the current event metadata via
 * {@link #getModel()} without changing method signatures.
 * </p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public class CanalContext {

    private static TransmittableThreadLocal<CanalModel> threadLocal = new TransmittableThreadLocal<>();

    /**
     * @return the Canal model bound to the current thread, or {@code null} if none is set
     */
    public static CanalModel getModel(){
        return threadLocal.get();
    }


    /**
     * Binds the given Canal model to the current thread.
     *
     * @param canalModel the model to bind (may be {@code null})
     */
    public static void setModel(CanalModel canalModel){
        threadLocal.set(canalModel);
    }


    /**
     * Removes the Canal model bound to the current thread.
     */
    public  static void removeModel(){
        threadLocal.remove();
    }
}
