package com.consistency.manager;

import com.consistency.model.ConsistencyTaskInstance;
import com.consistency.service.TaskStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * 任务调度管理器
 *
 * @author wzw
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TaskScheduleManager {
    
    private final TaskStoreService taskStoreService;
    
    private final CompletionService<ConsistencyTaskInstance> consistencyTaskPool;
    
    private final TaskEngineExecutor taskEngineExecutor;
    
    /**
     * 调度任务，需要在业务代码中自定义调度
     *
     * @throws InterruptedException
     */
    public void performConsistencyTask() throws InterruptedException {
        // 获取未完成任务列表
        List<ConsistencyTaskInstance> consistencyTaskInstances = taskStoreService.listByUnFinishTask();
        if (!CollectionUtils.isEmpty(consistencyTaskInstances)) {
            return;
        }
        // 过滤任务
        consistencyTaskInstances = consistencyTaskInstances.stream()
                //未到运行时间
                .filter(v -> v.getExecuteTime() - System.currentTimeMillis() <= 0)
                .collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(consistencyTaskInstances)) {
            return;
        }
        // 执行任务
        CountDownLatch latch = new CountDownLatch(consistencyTaskInstances.size());
        for (ConsistencyTaskInstance taskInstance : consistencyTaskInstances) {
            consistencyTaskPool.submit(() -> {
               try {
                   taskEngineExecutor.executeTaskInstance(taskInstance);
                   return taskInstance;
               } finally {
                   latch.countDown();
               }
            });
        }
        latch.await();
        log.info("[consistency framework] execute finished");
    }
}
