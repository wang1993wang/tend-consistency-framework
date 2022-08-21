package com.consistency.service;

import com.consistency.model.ConsistencyTaskInstance;

import java.util.List;

/**
 * 一致性存储任务的service接口
 *
 * @author wzw
 */
public interface TaskStoreService {
    
    /**
     * 保存最终一致性任务实例
     *
     * @param instance 实例信息
     */
    void initTask(ConsistencyTaskInstance instance);
    
    /**
     * 根据id获取任务实例信息
     *
     * @param id       任务id
     * @param shardKey 任务分片键
     * @return 任务实例信息
     */
    ConsistencyTaskInstance getTaskByIdAndShardKey(Long id, Long shardKey);
    
    /**
     * 获取未完成的任务
     *
     * @return 未完成任务的结果集
     */
    List<ConsistencyTaskInstance> listByUnFinishTask();
    
    /**
     * 启动任务
     *
     * @param consistencyTaskInstance 任务实例信息
     * @return 启动任务的结果
     */
    boolean turnOnTask(ConsistencyTaskInstance consistencyTaskInstance);
    
    /**
     * 标记任务成功
     *
     * @param consistencyTaskInstance 任务实例信息
     * @return 标记结果
     */
    int markSuccess(ConsistencyTaskInstance consistencyTaskInstance);
    
    /**
     * 标记任务为失败
     *
     * @param consistencyTaskInstance 一致性任务实例
     * @return 标记结果
     */
    boolean markFail(ConsistencyTaskInstance consistencyTaskInstance);
    
    /**
     * 标记为降级失败
     *
     * @param consistencyTaskInstance 一致性任务实例
     * @return 标记结果
     */
    boolean markFallbackFail(ConsistencyTaskInstance consistencyTaskInstance);
    
    /**
     * 提交任务实例信息
     *
     * @param taskInstance 任务实例信息
     */
    void submitTaskInstance(ConsistencyTaskInstance taskInstance);
}
