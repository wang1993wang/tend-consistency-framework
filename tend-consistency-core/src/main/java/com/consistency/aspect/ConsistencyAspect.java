package com.consistency.aspect;

import cn.hutool.core.util.ReflectUtil;
import cn.hutool.json.JSONUtil;
import com.consistency.annotation.ConsistencyTask;
import com.consistency.config.TendConsistencyConfiguration;
import com.consistency.custom.shard.SnowflakeShardingKeyGenerator;
import com.consistency.enums.ConsistencyTaskStatusEnum;
import com.consistency.enums.PerformanceEnum;
import com.consistency.model.ConsistencyTaskInstance;
import com.consistency.service.TaskStoreService;
import com.consistency.utils.ReflectTools;
import com.consistency.utils.TimeUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;

/**
 * 一致性任务切面
 *
 * @author wzw
 */
@Slf4j
@Aspect
@Component
@RequiredArgsConstructor
public class ConsistencyAspect {
    
    /**
     * 缓存生成任务分片key的对象实例
     */
    private static Object cacheGenerateShardKeyClassInstance = null;
    
    /**
     * 缓存生成任务分片key的方法
     */
    private static Method cacheGenerateShardKeyMethod = null;
    
    /**
     * 框架配置类
     */
    private final TendConsistencyConfiguration tendConsistencyConfiguration;
    
    /**
     * 一致性存储任务类
     */
    private final TaskStoreService taskStoreService;
    
    /**
     * 目标方法执行前置处理
     * @param point           目标方法
     * @param consistencyTask 一致性任务注解
     * @return
     */
    @Around("@annotation(consistencyTask)")
    public Object markConsistencyTask(ProceedingJoinPoint point, ConsistencyTask consistencyTask) {
        log.info("[consistency framework] access method:{} is called on {} args {}", point.getSignature().getName(), point.getThis(),
                point.getArgs());
        
        //构造任务实例
        ConsistencyTaskInstance instance = createInstance(consistencyTask, point);
    
        //任务入库
        taskStoreService.initTask(instance);
        
        //无论是立即执行还是调度执行的任务，都不在此对目标方法进行访问
        return null;
    }
    
    /**
     * 根据注解构造构造最终一致性任务的实例
     *
     * @param task  一致性任务注解信息 相当于任务的模板
     * @param point 方法切入点
     * @return 一致性任务实例
     */
    private ConsistencyTaskInstance createInstance(ConsistencyTask task, JoinPoint point) {
        // 获取入参类型
        Class<?>[] argsClazz = ReflectTools.getArgsClass(point.getArgs());
        // 获取被拦截方法的全先定名称 格式：类路径#方法名（参数1的类型,参数2的类型，...参数N的类型）
        String fullyQualifiedName = ReflectTools.getTargetMethodFullyQualifiedName(point, argsClazz);
        // 获取入参的类名称数组
        String parameterTypes = ReflectTools.getArgsClassNames(point.getSignature());
        
        Date date = new Date();
        
        ConsistencyTaskInstance instance = ConsistencyTaskInstance.builder()
                .taskId(StringUtils.isEmpty(task.id()) ? fullyQualifiedName : task.id())
                .methodSignName(fullyQualifiedName)
                .methodName(point.getSignature().getName())
                .parameterTypes(parameterTypes)
                .taskParameter(JSONUtil.toJsonStr(point.getArgs()))
                .taskStatus(ConsistencyTaskStatusEnum.INIT.getCode())
                .executeIntervalSec(task.executeIntervalSec())
                .delayTime(task.delayTime())
                .executeTimes(0)
                .errorMsg("")
                .performanceWay(task.performanceWay().getCode())
                .threadWay(task.threadWay().getCode())
                .alertExpression(StringUtils.isEmpty(task.alertExpression()) ? "" : task.alertExpression())
                .alertActionBeanName(StringUtils.isEmpty(task.alertActionBeanName()) ? "" : task.alertActionBeanName())
                .fallbackClassName(ReflectTools.getFullyQualifiedClassName(task.fallbackClass()))
                .fallbackErrorMsg("")
                .gmtCreate(date)
                .gmtModified(date)
                .build();
        // 设置执行时间
        instance.setExecuteTime(getExecuteTime(instance));
        // 设置分片键
        instance.setShardKey(tendConsistencyConfiguration.getTaskSharded() ? generateSharedKey() : 0l);
        
        return instance;
    }
    
    /**
     * 获取任务执行时间
     *
     * @param taskInstance 一致性任务实例
     * @return 下次执行时间
     */
    private Long getExecuteTime(ConsistencyTaskInstance taskInstance) {
        if (PerformanceEnum.PERFORMANCE_SCHEDULE.getCode().equals(taskInstance.getPerformanceWay())) {
            // 调度执行
            long delayTimeMillSecond = TimeUtils.secToMill(taskInstance.getDelayTime());
            return System.currentTimeMillis() + delayTimeMillSecond;
        } else {
            // 立即执行
            return System.currentTimeMillis();
        }
    }
    
    /**
     * 获取分片键
     *
     * @return 生成分片键
     */
    private Long generateSharedKey() {
        // 未配置任务分片键生成类，使用默认snowflake算法
        if (StringUtils.isEmpty(tendConsistencyConfiguration.getShardingKeyGeneratorClassName())) {
            return SnowflakeShardingKeyGenerator.getInstance().generateShardKey();
        }
        if (!ObjectUtils.isEmpty(cacheGenerateShardKeyMethod)
                && !ObjectUtils.isEmpty(cacheGenerateShardKeyClassInstance)) {
            try {
                return (Long) cacheGenerateShardKeyMethod.invoke(cacheGenerateShardKeyClassInstance);
            } catch (IllegalAccessException | InvocationTargetException e) {
                log.error("try to generate shard key with configured ShardingKeyGeneratorClass failed: {}", e);
            }
        }
        Class<?> shardingKeyGeneratorClass = getUserCustomShardingKeyGenerator();
        if (!ObjectUtils.isEmpty(shardingKeyGeneratorClass)) {
            String method = "generateShardKey";
            Method generateShardKeyMethod = ReflectUtil.getMethod(shardingKeyGeneratorClass, method);
            try {
                Constructor<?> constructor = ReflectUtil.getConstructor(shardingKeyGeneratorClass);
                cacheGenerateShardKeyClassInstance = constructor.newInstance();
                cacheGenerateShardKeyMethod = generateShardKeyMethod;
                return (Long) cacheGenerateShardKeyMethod.invoke(cacheGenerateShardKeyClassInstance);
            } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
                log.error("try to generate shard key with configured ShardingKeyGeneratorClass failed: {}", e);
                return SnowflakeShardingKeyGenerator.getInstance().generateShardKey();
            }
        }
        return SnowflakeShardingKeyGenerator.getInstance().generateShardKey();
    }
    
    /**
     * 获取ShardingKeyGenerator的实现类
     */
    private Class<?> getUserCustomShardingKeyGenerator() {
        return ReflectTools.getClassByName(tendConsistencyConfiguration.getShardingKeyGeneratorClassName());
    }
}
