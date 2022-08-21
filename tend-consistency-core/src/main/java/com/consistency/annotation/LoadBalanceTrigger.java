package com.consistency.annotation;

import lombok.RequiredArgsConstructor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import static com.consistency.config.Constant.REGISTER_PATH;

@RequiredArgsConstructor
public class LoadBalanceTrigger implements ApplicationRunner {
    
    private final CuratorFramework consistencyCuratorFramework;
    
    
    
    @Override
    public void run(ApplicationArguments args) throws Exception {
        election();
    }
    
    public void election() throws Exception {
        consistencyCuratorFramework.create().creatingParentContainersIfNeeded().withProtection()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(REGISTER_PATH);
        
    }
}
