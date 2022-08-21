package com.consistency.manager;

import com.consistency.model.ConsistencyTaskInstance;

/**
 * 任务执行引擎接口
 *
 * @author wzw
 */
public interface TaskEngineExecutor {
    
    /**
     * 执行任务实例
     *
     * @param taskInstance 任务实例
     */
    void executeTaskInstance(ConsistencyTaskInstance taskInstance);
    
    /**
     * 执行降级任务
     *
     * @param taskInstance 任务实例
     */
    void executeFallbackTask(ConsistencyTaskInstance taskInstance);
}
