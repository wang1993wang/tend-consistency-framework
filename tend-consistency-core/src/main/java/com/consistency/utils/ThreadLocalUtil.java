package com.consistency.utils;

/**
 * 线程标志工具类
 *
 * @author wzw
 */
public class ThreadLocalUtil {
    
    /**
     * ACTION被AOP拦截时是否应该立即执行任务
     */
    private static final ThreadLocal<Boolean> FLAG = ThreadLocal.withInitial(() -> false);
    
    public static void setFlag(boolean flag) {
        FLAG.set(flag);
    }
    
    /**
     * 是否为执行器在执行
     * @return
     */
    public static Boolean getFlag() {
        return FLAG.get();
    }
}
