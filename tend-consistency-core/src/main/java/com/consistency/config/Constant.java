package com.consistency.config;

/**
 * 静态数据配置类
 *
 * @author wzw
 */
public class Constant {
    
    /**
     * 节点注册路径
     */
    public static final String REGISTER_PATH = "/noderegister";
    
    /**
     * 一致性任务线程名称前缀
     */
    public static final String CONSISTENCY_TASK_THREAD_POOL_PREFIX = "CTThreadPool_";
    
    /**
     * 告警线程名称的前缀
     */
    public static final String ALERT_THREAD_POOL_PREFIX = "AlertThreadPool_";
    
    /**
     * 任务幂等性redis key前缀
     */
    public static final String KEY_FORMAT = "TEND:TASK:%";
    
    /**
     * zk命名空间
     */
    public static final String NAMESPACE = "tendconsistency";
    
    /**
     * zk分布式锁根目录
     */
    public static final String LOCK_ROOT_PATH = "/tasklock";
    
}
