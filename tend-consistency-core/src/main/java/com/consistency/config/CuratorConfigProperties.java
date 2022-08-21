package com.consistency.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * curator配置类
 *
 * @author wzw
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ConfigurationProperties(prefix = "tend.consistency.zk")
public class CuratorConfigProperties {
    
    /**
     * 重试次数
     */
    public Integer retryCount = 5;
    
    /**
     * 重试等待时间
     */
    public Integer elapsedTimeMs = 5000;
    
    /**
     * 连接地址
     */
    public String connectString;
    
    /**
     * 会话超时时间
     */
    public Integer sessionTimeoutMs = 60000;
    
    /**
     * 连接超时时间
     */
    public Integer connectionTimeoutMs = 5000;
}
