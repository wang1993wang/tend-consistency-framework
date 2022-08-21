package com.consistency.service;

import com.consistency.config.TendConsistencyConfiguration;
import com.consistency.custom.query.TaskTimeRangeQuery;
import com.consistency.enums.ConsistencyTaskStatusEnum;
import com.consistency.enums.PerformanceEnum;
import com.consistency.enums.ThreadWayEnum;
import com.consistency.exceptions.ConsistencyException;
import com.consistency.manager.TaskExecutor;
import com.consistency.mapper.TaskOperateService;
import com.consistency.model.ConsistencyTaskInstance;
import com.consistency.utils.ReflectTools;
import com.consistency.utils.SpringUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.StringUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;

/**
 * 任务存储的service实现类
 *
 * @author wzw
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TaskStoreServiceImpl implements TaskStoreService {
    
    private final TaskOperateService taskOperateService;
    
    private final CompletionService<ConsistencyTaskInstance> consistencyTaskPool;
    
    private final TendConsistencyConfiguration consistencyConfiguration;
    
    private final TaskExecutor taskExecutor;
    
    @Override
    public void initTask(ConsistencyTaskInstance taskInstance) {
        Integer result = taskOperateService.initTask(taskInstance);
        log.info("[consistency framework] init task result [{}]", result > 0);
        
        if (PerformanceEnum.PERFORMANCE_RIGHT_NOW.getCode().equals(taskInstance.getPerformanceWay())) {
            return;
        }
        
        boolean synchronizationActive = TransactionSynchronizationManager.isSynchronizationActive();
        if (synchronizationActive) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
                @Override
                public void afterCommit() {
                    submitTaskInstance(taskInstance);
                }
            });
        } else {
            submitTaskInstance(taskInstance);
        }
    }
    
    @Override
    public ConsistencyTaskInstance getTaskByIdAndShardKey(Long id, Long shardKey) {
        return taskOperateService.getTaskByIdAndShardKey(id, shardKey);
    }
    
    @Override
    public List<ConsistencyTaskInstance> listByUnFinishTask() {
        Date startTime, endTime;
        Long limitTaskCount;
        try {
            // 获取TaskTimeLineQuery实现类
            if (!StringUtils.isEmpty(consistencyConfiguration.getTaskScheduleTimeRangeClassName())) {
                Map<String, TaskTimeRangeQuery> beansOfTypeMap = SpringUtil.getBeansOfType(TaskTimeRangeQuery.class);
                TaskTimeRangeQuery taskTimeRangeQuery = getTaskTimeRangeQuery(beansOfTypeMap);
                startTime = taskTimeRangeQuery.getStartTime();
                endTime = taskTimeRangeQuery.getEndTime();
                limitTaskCount = taskTimeRangeQuery.limitTaskCount();
            } else {
                startTime = TaskTimeRangeQuery.getStartTimeByStatic();
                endTime = TaskTimeRangeQuery.getEndTimeByStatic();
                limitTaskCount = TaskTimeRangeQuery.limitTaskCountByStatic();
            }
        } catch (Exception e) {
            log.error("[consistency framework] list not finished tasks fail, detail is ", e);
            throw new ConsistencyException(e);
        }
        return taskOperateService.listByUnFinishTask(startTime.getTime(), endTime.getTime(), limitTaskCount);
    }
    
    /**
     * 获取TaskTimeRangeQuery实现类
     *
     * @param beansOfTypeMap TaskTimeRangeQuery接口实现类的集合
     * @return TaskTimeRangeQuery接口实现类
     */
    private TaskTimeRangeQuery getTaskTimeRangeQuery(Map<String, TaskTimeRangeQuery> beansOfTypeMap) {
        if (beansOfTypeMap.size() == 1) {
            String[] beanNamesForType = SpringUtil.getBeanNamesForType(TaskTimeRangeQuery.class);
            return SpringUtil.getBean(beanNamesForType[0]);
        }
        Class<?> clazz = ReflectTools.getClassByName(consistencyConfiguration.getTaskScheduleTimeRangeClassName());
        return (TaskTimeRangeQuery) SpringUtil.getBean(clazz);
    }
    
    @Override
    @Transactional(rollbackFor = Exception.class, propagation = Propagation.REQUIRES_NEW)
    public boolean turnOnTask(ConsistencyTaskInstance taskInstance) {
        //任务实际运行时间
        taskInstance.setExecuteTime(System.currentTimeMillis());
        taskInstance.setTaskStatus(ConsistencyTaskStatusEnum.START.getCode());
        return taskOperateService.turnOnTask(taskInstance);
    }
    
    @Override
    @Transactional(rollbackFor = Exception.class)
    public int markSuccess(ConsistencyTaskInstance taskInstance) {
        return taskOperateService.markSuccess(taskInstance);
    }
    
    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean markFail(ConsistencyTaskInstance taskInstance) {
        return taskOperateService.markFail(taskInstance);
    }
    
    @Override
    public boolean markFallbackFail(ConsistencyTaskInstance taskInstance) {
        return taskOperateService.markFallbackFail(taskInstance);
    }
    
    @Override
    public void submitTaskInstance(ConsistencyTaskInstance taskInstance) {
        if (ThreadWayEnum.SYNC.getCode().equals(taskInstance.getThreadWay())) {
            taskExecutor.executeTask(taskInstance);
        } else if (ThreadWayEnum.ASYNC.getCode().equals(taskInstance.getThreadWay())) {
            consistencyTaskPool.submit(() -> {
                taskExecutor.executeTask(taskInstance);
                return taskInstance;
            });
        }
    }
}
