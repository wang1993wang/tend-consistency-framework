package com.consistency.utils;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

import static com.consistency.config.Constant.LOCK_ROOT_PATH;

/**
 * 分布式锁工具类
 *
 * @author wzw
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DistributedLockByCurator {
    
    private final CuratorFramework consistencyCuratorFramework;
    
    private InterProcessLock lock = null;
    
    private ThreadLocal<Boolean> lockStatus = ThreadLocal.withInitial(() -> false);
    
    /**
     * 获取分布式锁
     *
     * @param path
     */
    public boolean acquireDistributedLock(String path) {
        String keyPath = LOCK_ROOT_PATH + "/" + path;
        try {
            lock = new InterProcessMutex(consistencyCuratorFramework, keyPath);
            lock.acquire();
            lockStatus.set(true);
            log.info("[consistency framework] success to acquire lock for path: {}", keyPath);
        } catch (Exception e) {
            log.info("[consistency framework] failed to acquire lock for path:{}", keyPath);
            return false;
        }
        return true;
    }
    
    /**
     * 释放分布式锁
     *
     * @return
     */
    public boolean releaseDistributedLock() {
        try {
            if (lockStatus.get()) {
                lock.release();
            } else {
                return false;
            }
        } catch (Exception e) {
            log.error("[consistency framework] failed to release lock", e);
            return false;
        }
        return true;
    }
}
