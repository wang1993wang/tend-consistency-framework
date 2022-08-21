package com.consistency.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * redis配置类
 *
 * @author wzw
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ConfigurationProperties(prefix = "tend.consistency.redis")
public class RedisConfigProperties {
    
    /**
     * 集群模式
     */
    public String node;
    
    /**
     * 哨兵模式
     */
    public String sentinel;
    
    /**
     * 数据库索引
     */
    public Integer database = 0;
    
    /**
     * 服务器连接密码
     */
    public String password = "";
    
    /**
     * 客户端超时时间，单位毫秒
     */
    public Integer timeout = 3000;
    
    /**
     * 连接池最小空闲数
     */
    public Integer minIdle = 10;
    
    /**
     * 连接池最大空闲数
     */
    public Integer maxIdle = 20;
    
    /**
     * 连接池总连接数
     */
    public Integer maxTotal = 50;
    
    /**
     * 重定向最大数量
     */
    public Integer maxRedirect = 5;
    
    /**
     * 连接最大等待时间
     */
    public Integer maxWaitMillis = 1000;
    
    /**
     * 逐出连接最小空闲时间
     */
    public Integer minEvictableIdleTimeMillis = 300000;
    
    /**
     * 每次逐出空闲连接的最大数量
     */
    public Integer numTestsPerEvictionRun = 10;
    
    /**
     * 逐出连接的扫描间隔
     */
    public Long timeBetweenEvictionRunsMillis = 30000l;
    
    /**
     * 是否在池中取出连接前进行检验，检验失败，则尝试取另一个
     */
    public Boolean testOnBorrow = true;
    
    /**
     * 空闲时间检查有效性
     */
    public Boolean testWhileIdle = true;
}
