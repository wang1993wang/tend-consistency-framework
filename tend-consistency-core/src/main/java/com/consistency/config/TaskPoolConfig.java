package com.consistency.config;

import com.consistency.model.ConsistencyTaskInstance;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.consistency.config.Constant.*;

/**
 * 线程池配置类
 *
 * @author wzw
 */
@Configuration
@RequiredArgsConstructor
public class TaskPoolConfig {
    
    /**
     * 框架配置
     */
    private final TendConsistencyConfiguration tendConsistencyConfiguration;
    
    /**
     * 并行任务执行线程池
     *
     * @return 并行任务执行线程池
     */
    @Bean
    public CompletionService<ConsistencyTaskInstance> consistencyTaskPool() {
        LinkedBlockingQueue<Runnable> asyncConsistencyTaskThreadPoolQueue =
                new LinkedBlockingQueue<>(tendConsistencyConfiguration.threadPoolQueueSize);
        ThreadPoolExecutor asyncReleaseResourceExecutorPool = new ThreadPoolExecutor(
                tendConsistencyConfiguration.getThreadCorePoolSize(),
                tendConsistencyConfiguration.getThreadMaxPoolSize(),
                tendConsistencyConfiguration.getThreadPoolKeepAliveTime(),
                TimeUnit.valueOf(tendConsistencyConfiguration.getThreadPoolKeepAliveTimeUnit()),
                asyncConsistencyTaskThreadPoolQueue,
                createThreadFactory(CONSISTENCY_TASK_THREAD_POOL_PREFIX));
        return new ExecutorCompletionService<>(asyncReleaseResourceExecutorPool);
    }
    
    /**
     * 告警任务线程池
     *
     * @return 告警任务线程池
     */
    @Bean
    public ThreadPoolExecutor alertNoticePool() {
        LinkedBlockingQueue<Runnable> asyncAlertNoticeThreadPoolQueue =
                new LinkedBlockingQueue<>(100);
        return new ThreadPoolExecutor(
                3,
                5,
                60,
                TimeUnit.SECONDS,
                asyncAlertNoticeThreadPoolQueue,
                createThreadFactory(ALERT_THREAD_POOL_PREFIX));
    }
    
    /**
     * 线程池工厂
     *
     * @param threadNamePrefix 线程名前缀
     * @return 线程池工厂
     */
    private ThreadFactory createThreadFactory(String threadNamePrefix) {
        return new ThreadFactory() {
            
            private AtomicInteger threadIndex = new AtomicInteger(0);
            
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, threadNamePrefix + this.threadIndex.incrementAndGet());
            }
        };
    }
    
}
