package com.consistency.mapper;

import com.baomidou.mybatisplus.extension.conditions.query.LambdaQueryChainWrapper;
import com.baomidou.mybatisplus.extension.conditions.update.LambdaUpdateChainWrapper;
import com.consistency.model.ConsistencyTaskInstance;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据库操作
 *
 * @author wzw
 */
@Service
@RequiredArgsConstructor
public class TaskOperateService {
    
    private final TaskStoreMapperImpl taskStoreMapperImpl;
    
    public int initTask(ConsistencyTaskInstance taskInstance) {
        return taskStoreMapperImpl.getBaseMapper().insert(taskInstance);
    }
    
    public boolean turnOnTask(ConsistencyTaskInstance taskInstance) {
        return new LambdaUpdateChainWrapper<>(taskStoreMapperImpl.getBaseMapper())
                .eq(ConsistencyTaskInstance::getId, taskInstance.getId())
                .ne(ConsistencyTaskInstance::getTaskStatus, 1)
                .eq(ConsistencyTaskInstance::getShardKey, taskInstance.getShardKey())
                .set(ConsistencyTaskInstance::getTaskStatus, taskInstance.getTaskStatus())
                .set(ConsistencyTaskInstance::getExecuteTimes, taskInstance.getExecuteTimes() + 1)
                .set(ConsistencyTaskInstance::getExecuteTime, taskInstance.getExecuteTime())
                .update();
    }
    
    public ConsistencyTaskInstance getTaskByIdAndShardKey(Long id, Long shardKey) {
        return new LambdaQueryChainWrapper<>(taskStoreMapperImpl.getBaseMapper())
                .eq(ConsistencyTaskInstance::getId, id)
                .eq(ConsistencyTaskInstance::getShardKey, shardKey)
                .one();
                
    }
    
    public Integer markSuccess(ConsistencyTaskInstance taskInstance) {
        Map<String, Object> conditionMap = new HashMap<>();
        conditionMap.put("id", taskInstance.getId());
        conditionMap.put("shard_key", taskInstance.getShardKey());
        return taskStoreMapperImpl.getBaseMapper().deleteByMap(conditionMap);
    }
    
    public boolean markFail(ConsistencyTaskInstance taskInstance) {
        return new LambdaUpdateChainWrapper<>(taskStoreMapperImpl.getBaseMapper())
                .eq(ConsistencyTaskInstance::getId, taskInstance.getId())
                .eq(ConsistencyTaskInstance::getShardKey, taskInstance.getShardKey())
                .set(ConsistencyTaskInstance::getTaskStatus, 2)
                .set(ConsistencyTaskInstance::getErrorMsg, taskInstance.getErrorMsg())
                .set(ConsistencyTaskInstance::getExecuteTime, taskInstance.getExecuteTime())
                .update();
    }
    
    public boolean markFallbackFail(ConsistencyTaskInstance taskInstance) {
        return new LambdaUpdateChainWrapper<>(taskStoreMapperImpl.getBaseMapper())
                .eq(ConsistencyTaskInstance::getId, taskInstance.getId())
                .eq(ConsistencyTaskInstance::getShardKey, taskInstance.getShardKey())
                .set(ConsistencyTaskInstance::getErrorMsg, taskInstance.getErrorMsg())
                .update();
    }
    
    public List<ConsistencyTaskInstance> listByUnFinishTask(Long startTime, Long endTime, Long limitTaskCount) {
        return new LambdaQueryChainWrapper<>(taskStoreMapperImpl.getBaseMapper())
                .le(ConsistencyTaskInstance::getTaskStatus, 2)
                .ge(ConsistencyTaskInstance::getExecuteTime, startTime)
                .le(ConsistencyTaskInstance::getExecuteTime, endTime)
                .orderByDesc(ConsistencyTaskInstance::getExecuteTime)
                .apply("limit {0}", limitTaskCount)
                .list();
    }
}
