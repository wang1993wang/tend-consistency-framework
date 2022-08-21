package com.consistency.manager;

import cn.hutool.core.util.ReflectUtil;
import cn.hutool.json.JSONUtil;
import com.consistency.config.TendConsistencyConfiguration;
import com.consistency.custom.alerter.ConsistencyFrameworkAlerter;
import com.consistency.exceptions.ConsistencyException;
import com.consistency.model.ConsistencyTaskInstance;
import com.consistency.service.TaskStoreService;
import com.consistency.utils.DistributedLockByCurator;
import com.consistency.utils.ReflectTools;
import com.consistency.utils.SpringUtil;
import com.consistency.utils.TimeUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.consistency.config.Constant.KEY_FORMAT;
import static com.consistency.utils.ExpressionUtils.RESULT_FLAG;
import static com.consistency.utils.ExpressionUtils.buildDataMap;
import static com.consistency.utils.ExpressionUtils.readExpr;
import static com.consistency.utils.ExpressionUtils.rewriteExpr;

@Slf4j
@Component
@RequiredArgsConstructor
public class TaskEngineExecutorImpl implements TaskEngineExecutor {
    
    private final TaskStoreService taskStoreService;
    
    private final ThreadPoolExecutor alertNoticePool;
    
    private final TendConsistencyConfiguration consistencyConfiguration;
    
    private final TaskExecutor taskExecutor;
    
    private final DistributedLockByCurator distributedLock;
    
    private final RedisTemplate<String, Object> consistencyRedisTemplate;
    
    private String taskKey = null;
    
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void executeTaskInstance(ConsistencyTaskInstance taskInstance) {
        try {
            if (!distributedLock.acquireDistributedLock(String.valueOf(taskInstance.getId()))) {
                log.info("[consistency framework] current task [{}] is running", taskInstance.getId());
                return;
            }
            
            // 幂等检查
            taskKey = String.format(KEY_FORMAT, taskInstance.getId());
            if (consistencyRedisTemplate.hasKey(taskKey)) {
                try {
                    taskStoreService.markSuccess(taskInstance);
                } catch (Exception e) {
                    log.error("[consistency framework] finished task remark success failed", e);
                    // 续期
                    consistencyRedisTemplate.expire(taskKey, 2, TimeUnit.DAYS);
                    return;
                }
                consistencyRedisTemplate.delete(taskKey);
                return;
            }
            
            boolean result = taskStoreService.turnOnTask(taskInstance);
            if (!result) {
                log.warn("[consistency framework] task was already started, task:{}", JSONUtil.toJsonStr(taskInstance));
                distributedLock.releaseDistributedLock();
                return;
            }
    
            // 获取最新任务信息
            taskInstance = taskStoreService.getTaskByIdAndShardKey(taskInstance.getId(), taskInstance.getShardKey());
    
            // 执行任务
            taskExecutor.executeTask(taskInstance);
    
            // 删除任务
            int successResult = 0;
            successResult = taskStoreService.markSuccess(taskInstance);
            consistencyRedisTemplate.opsForValue().set(taskKey, taskKey);
            consistencyRedisTemplate.expire(taskKey, 2, TimeUnit.DAYS);
            log.info("[consistency framework] task execute result is [{}]", successResult > 0);
        } catch (Exception e) {
            log.error("[consistency framework] {} execute task failed, cause is ", JSONUtil.toJsonStr(taskInstance), e);
            taskInstance.setErrorMsg(getErrorMsg(e));
            taskInstance.setExecuteTime(getNextExecuteTime(taskInstance));
            taskStoreService.markFail(taskInstance);
            log.info("[consistency framework] failed task will be scheduled in {}", getFormatTime(taskInstance.getExecuteTime()));
            executeFallbackTask(taskInstance);
        } finally {
            distributedLock.releaseDistributedLock();
        }
    }
    
    @Override
    public void executeFallbackTask(ConsistencyTaskInstance taskInstance) {
        // 无降级类，触发告警
        if (StringUtils.isEmpty(taskInstance.getFallbackClassName())) {
            parseExpressionAndDoAlert(taskInstance);
            return;
        }
        // 未满足降级阈值
        if (taskInstance.getExecuteTimes() <= consistencyConfiguration.getFailCountThreshold()) {
            return;
        }
        log.info("[consistency framework] execute task {} fallback action", taskInstance.getId());
        Class<?> fallbackClass = ReflectTools.getClassByName(taskInstance.getFallbackClassName());
        if (ObjectUtils.isEmpty(fallbackClass)) {
            return;
        }
        // 执行降级
        String taskParameterText = taskInstance.getTaskParameter();
        String parameterTypes = taskInstance.getParameterTypes();
        Class<?>[] paramTypes = ReflectTools.getArgsClass(parameterTypes.split(","));
        Object[] paramValues = ReflectTools.buildArgs(taskParameterText, paramTypes);
        Object fallbackClassBean = SpringUtil.getBean(fallbackClass, paramValues);
        Method fallbackMethod = ReflectUtil.getMethod(fallbackClass, taskInstance.getMethodName(), paramTypes);
        try {
            fallbackMethod.invoke(fallbackClassBean, paramValues);
            taskStoreService.markSuccess(taskInstance);
            consistencyRedisTemplate.opsForValue().set(taskKey, taskKey);
            consistencyRedisTemplate.expire(taskKey, 2, TimeUnit.DAYS);
            log.info("[consistency framework] fallback action executed successful");
        } catch (Exception e) {
            parseExpressionAndDoAlert(taskInstance);
            taskInstance.setFallbackErrorMsg(getErrorMsg(e));
            taskStoreService.markFallbackFail(taskInstance);
            log.error("[consistency framework] fallback action executed fail, next executed time is [{}]", getFormatTime(taskInstance.getExecuteTime()));
        }
    }
    
    /**
     * 解析并对表达式结果进行校验，并执行相关的告警逻辑
     *
     * @param taskInstance 任务实例信息
     */
    private void parseExpressionAndDoAlert(ConsistencyTaskInstance taskInstance) {
        try {
            if (StringUtils.isEmpty(taskInstance.getAlertExpression())) {
                return;
            }
            alertNoticePool.submit(() -> {
                String expr = rewriteExpr(taskInstance.getAlertExpression());
                String exprResult = readExpr(expr, buildDataMap(taskInstance));
                doAlert(exprResult, taskInstance);
            });
        } catch (Exception e) {
            log.error("[consistency framework] send alert notice failed, detail is ", e);
        }
    }
    
    private void doAlert(String exprResult, ConsistencyTaskInstance taskInstance) {
        if (StringUtils.isEmpty(exprResult)) {
            return;
        }
        if (!RESULT_FLAG.equals(exprResult)) {
            return;
        }
        log.warn("[consistency framework] task {} triggered alert rule, please check", taskInstance.getId());
        if (!StringUtils.isEmpty(taskInstance.getAlertActionBeanName())) {
            return;
        }
        sendAlertNotice(taskInstance);
    }
    
    private void sendAlertNotice(ConsistencyTaskInstance taskInstance) {
        Map<String, ConsistencyFrameworkAlerter> beansOfTypeMap = SpringUtil.getBeansOfType(
                ConsistencyFrameworkAlerter.class);
        if (CollectionUtils.isEmpty(beansOfTypeMap)) {
            log.warn("[consistency framework] can not get the implementations of ConsistencyFrameworkAlerter");
            return;
        }
        try {
            getConsistencyFrameworkAlerterImpl(taskInstance, beansOfTypeMap).sendAlertNotice(taskInstance);
        } catch (Exception e) {
            log.error("[consistency framework] invoke implementations of ConsistencyFrameworkAlerter failed", e);
            throw new ConsistencyException(e);
        }
    }
    
    private ConsistencyFrameworkAlerter getConsistencyFrameworkAlerterImpl(ConsistencyTaskInstance taskInstance,
            Map<String, ConsistencyFrameworkAlerter> beansOfTypeMap) {
        if (beansOfTypeMap.size() == 1) {
            String[] beanNamesForType = SpringUtil.getBeanNamesForType(ConsistencyFrameworkAlerter.class);
            return SpringUtil.getBean(beanNamesForType[0]);
        }
        return beansOfTypeMap.get(taskInstance.getAlertActionBeanName());
    }
    
    /**
     * 获取任务下一次的执行时间
     *
     * @param taskInstance 一致性任务实例
     * @return 下次执行时间
     */
    private long getNextExecuteTime(ConsistencyTaskInstance taskInstance) {
        // 上次执行时间 + （下一次执行的次数 * 执行间隔）
        // 对于一个任务重试执行的时间间隔，是有一个参数配置
        // 第一次执行失败之后，会用第一次执行之前的时间 + （1 + 1） * 20s = 第一次执行时间往后推40s
        return taskInstance.getExecuteTime() + ((taskInstance.getExecuteTimes() + 1) *
                TimeUtils.secToMill(taskInstance.getExecuteIntervalSec()));
    }
    
    private String getFormatTime(long timestamp) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.format(timestamp);
    }
    
    /**
     * 获取异常信息
     *
     * @param e 异常对象
     * @return 异常信息
     */
    private String getErrorMsg(Exception e) {
        if ("".equals(e.getMessage())) {
            return "";
        }
        String errorMsg = e.getMessage();
        if (StringUtils.isEmpty(errorMsg)) {
            if (e instanceof IllegalAccessException) {
                IllegalAccessException illegalAccessException = (IllegalAccessException) e;
                errorMsg = illegalAccessException.getMessage();
            } else if (e instanceof IllegalArgumentException) {
                IllegalArgumentException illegalArgumentException = (IllegalArgumentException) e;
                errorMsg = illegalArgumentException.getMessage();
            } else if (e instanceof InvocationTargetException) {
                InvocationTargetException invocationTargetException = (InvocationTargetException) e;
                errorMsg = invocationTargetException.getTargetException().getMessage();
            }
        }
        return errorMsg.substring(0, Math.min(errorMsg.length(), 200));
    }
}
