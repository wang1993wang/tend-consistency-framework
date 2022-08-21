package com.consistency.manager;

import com.consistency.exceptions.ConsistencyException;
import com.consistency.model.ConsistencyTaskInstance;
import com.consistency.utils.ReflectTools;
import com.consistency.utils.SpringUtil;
import com.consistency.utils.ThreadLocalUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * 任务执行器
 *
 * @author wzw
 */
@Slf4j
@Component
public class TaskExecutor {
    
    /**
     * 执行指定任务
     *
     * @param taskInstance 任务实例信息
     */
    public void executeTask(ConsistencyTaskInstance taskInstance) {
        // 获取方法签名 格式：类路径#方法名(参数1的类型,参数2的类型,...参数N的类型)
        String methodSignName = taskInstance.getMethodSignName();
        // 获取目标类
        Class<?> clazz = ReflectTools.getTaskMethodClass(methodSignName);
        if (ObjectUtils.isEmpty(clazz)) {
            return;
        }
        // 获取目标对象
        Object bean = SpringUtil.getBean(clazz);
        if (ObjectUtils.isEmpty(bean)) {
            return;
        }
        // 获取目标方法
        String methodName = taskInstance.getMethodName();
        String[] parameterTypes = taskInstance.getParameterTypes().split(",");
        Class<?>[] parameterClasses = ReflectTools.getArgsClass(parameterTypes);
        Method targetMethod = ReflectTools.getTargetMethod(methodName, parameterClasses, clazz);
        if (ObjectUtils.isEmpty(targetMethod)) {
            return;
        }
        // 构造入参
        Object[] args = ReflectTools.buildArgs(taskInstance.getTaskParameter(), parameterClasses);
        try {
            ThreadLocalUtil.setFlag(true);
            //这里仍然是调用aop增强后的方法，所以使用threadlocal标记使其立即执行
            targetMethod.invoke(bean, args);
            ThreadLocalUtil.setFlag(false);
        } catch (InvocationTargetException e) {
            log.error("[consistency framework] invoke target method failed, detail is ", e);
            Throwable target = e.getTargetException();
            throw new ConsistencyException((Exception) target);
        } catch (Exception e) {
            throw new ConsistencyException(e);
        }
    }
}
