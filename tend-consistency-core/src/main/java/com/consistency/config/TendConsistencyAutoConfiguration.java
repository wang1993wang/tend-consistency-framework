package com.consistency.config;

import com.consistency.custom.query.TaskTimeRangeQuery;
import com.consistency.custom.shard.ShardingKeyGenerator;
import com.consistency.exceptions.ConsistencyException;
import com.consistency.utils.ReflectTools;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.consistency.config.Constant.NAMESPACE;
import static com.consistency.utils.DefaultValueUtils.getOrDefault;

/**
 * 提供给SpringBoot的自动装配类 SPI使用
 *
 * @author wzw
 **/
@Slf4j
@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(value = {
        TendConsistencyParallelTaskConfigProperties.class,
        TendConsistencyFallbackConfigProperties.class,
        ShardModeConfigProperties.class,
        CuratorConfigProperties.class,
        RedisConfigProperties.class
})
public class TendConsistencyAutoConfiguration {
    
    /**
     * 执行调度任务的线程池的配置
     */
    private final TendConsistencyParallelTaskConfigProperties tendConsistencyParallelTaskConfigProperties;
    /**
     * 降级逻辑相关参数配置
     */
    private final TendConsistencyFallbackConfigProperties tendConsistencyFallbackConfigProperties;
    /**
     * 分片模式参数配置
     */
    private final ShardModeConfigProperties shardModeConfigProperties;
    /**
     * curator参数配置
     */
    private final CuratorConfigProperties curatorConfigProperties;
    /**
     * redis参数配置
     */
    private final RedisConfigProperties redisConfigProperties;
    
    /**
     * 框架级配置
     *
     * @return 配置TendConsistencyConfiguration
     */
    @Bean
    public TendConsistencyConfiguration tendConsistencyConfigService() {
        doConfigCheck(tendConsistencyParallelTaskConfigProperties, shardModeConfigProperties);
        return TendConsistencyConfiguration.builder()
                .threadCorePoolSize(getOrDefault(tendConsistencyParallelTaskConfigProperties.getThreadCorePoolSize(), 5))
                .threadMaxPoolSize(getOrDefault(tendConsistencyParallelTaskConfigProperties.getThreadMaxPoolSize(), 5))
                .threadPoolQueueSize(getOrDefault(tendConsistencyParallelTaskConfigProperties.getThreadPoolQueueSize(), 100))
                .threadPoolKeepAliveTime(getOrDefault(tendConsistencyParallelTaskConfigProperties.getThreadPoolKeepAliveTime(), 60l))
                .threadPoolKeepAliveTimeUnit(getOrDefault(tendConsistencyParallelTaskConfigProperties.getThreadPoolKeepAliveTimeUnit(), "SECONDS"))
                .failCountThreshold(getOrDefault(tendConsistencyFallbackConfigProperties.getFailCountThreshold(), 5))
                .taskSharded(getOrDefault(shardModeConfigProperties.taskSharded, false))
                .taskScheduleTimeRangeClassName(getOrDefault(tendConsistencyParallelTaskConfigProperties.getTaskScheduleTimeRangeClassName(), ""))
                .shardingKeyGeneratorClassName(getOrDefault(shardModeConfigProperties.getShardingKeyGeneratorClassName(), ""))
                .build();
        
    }
    
    /**
     * curator配置
     *
     * @return consistencyCuratorFramework
     */
    @Bean
    public CuratorFramework consistencyCuratorFramework() {
        doCuratorConfigCheck(curatorConfigProperties);
        return CuratorFrameworkFactory.builder()
                .connectString(curatorConfigProperties.getConnectString())
                .sessionTimeoutMs(getOrDefault(curatorConfigProperties.getSessionTimeoutMs(), 5000))
                .connectionTimeoutMs(getOrDefault(curatorConfigProperties.getConnectionTimeoutMs(), 5000))
                .retryPolicy(new RetryNTimes(getOrDefault(curatorConfigProperties.getRetryCount(), 5), getOrDefault(curatorConfigProperties.getElapsedTimeMs(), 5000)))
                .namespace(NAMESPACE)
                .build();
    }
    
    /**
     * redisTemplate
     *
     * @param consistencyJedisConnectionFactory redis连接工厂
     * @return RedisTemplate
     */
    @Bean("consistencyRedisTemplate")
    public RedisTemplate<String, Object> consistencyRedisTemplate(@Qualifier("consistencyJedisConnectionFactory") JedisConnectionFactory consistencyJedisConnectionFactory){
        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
        initRedisTemplate(redisTemplate, consistencyJedisConnectionFactory);
        return redisTemplate;
    }
    
    /**
     * redis配置
     *
     * @return consistencyJedisConnectionFactory连接工厂
     */
    @Bean("consistencyJedisConnectionFactory")
    public JedisConnectionFactory consistencyJedisConnectionFactory(){
        doRedisConfigCheck(redisConfigProperties);
        // 连接池配置
        JedisPoolConfig jedisPoolConfig=new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(getOrDefault(redisConfigProperties.getMaxIdle(), 20));
        jedisPoolConfig.setMaxTotal(getOrDefault(redisConfigProperties.getMaxTotal(), 50));
        jedisPoolConfig.setMaxWaitMillis(getOrDefault(redisConfigProperties.getMaxWaitMillis(), 1000));
        jedisPoolConfig.setMinEvictableIdleTimeMillis(getOrDefault(redisConfigProperties.getMinEvictableIdleTimeMillis(), 300000));
        jedisPoolConfig.setNumTestsPerEvictionRun(getOrDefault(redisConfigProperties.getNumTestsPerEvictionRun(), 10));
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(getOrDefault(redisConfigProperties.getTimeBetweenEvictionRunsMillis(), 30000l));
        jedisPoolConfig.setTestOnBorrow(getOrDefault(redisConfigProperties.getTestOnBorrow(), true));
        jedisPoolConfig.setTestWhileIdle(getOrDefault(redisConfigProperties.getTestWhileIdle(), true));
        // 连接工厂
        JedisConnectionFactory factory = new JedisConnectionFactory(jedisPoolConfig);
        String node = redisConfigProperties.getNode();
        String sentinel = redisConfigProperties.getSentinel();
        String password = redisConfigProperties.getPassword();
        JedisClientConfiguration.JedisPoolingClientConfigurationBuilder jpcb =
                (JedisClientConfiguration.JedisPoolingClientConfigurationBuilder) JedisClientConfiguration.builder();
        // 指定jedisPoolConfig来修改默认的连接池构造器
        jpcb.poolConfig(jedisPoolConfig);
        // 通过构造器来构造jedis客户端配置
        JedisClientConfiguration jedisClientConfiguration = jpcb.build();
        String[] hostPortArr = node.split(",");
        Set<HostAndPort> nodes = new LinkedHashSet<>();
        for (int i = 0; i < hostPortArr.length; i++) {
            try {
                String[] hostPort = hostPortArr[i].split(":");
                nodes.add(new HostAndPort(hostPort[0], Integer.parseInt(hostPort[1])));
            } catch (Exception e) {
                String errMsg = String.format("出现配置错误!请确认node=[%s]是否正确", node);
                throw new ConsistencyException(errMsg);
            }
        }
        // sentinel
        if (!StringUtils.isEmpty(sentinel)) {
            log.info("[consistency framework] Redis use SentinelConfiguration");
            RedisSentinelConfiguration redisSentinelConfiguration = new RedisSentinelConfiguration();
            String[] sentinelArray = sentinel.split(",");
            for (String sentinelNode : sentinelArray) {
                try {
                    String[] hostPort = sentinelNode.split(":");
                    redisSentinelConfiguration.addSentinel(new RedisNode(hostPort[0], Integer.parseInt(hostPort[1])));
                } catch (Exception e) {
                    throw new RuntimeException(String.format("出现配置错误!请确认node=[%s]是否正确", node));
                }
            }
            factory = new JedisConnectionFactory(redisSentinelConfiguration, jedisClientConfiguration);
        }
        // standalone
        else if (nodes.size() == 1) {
            log.info("[consistency framework] Redis use RedisStandaloneConfiguration");
            for (HostAndPort n : nodes) {
                RedisStandaloneConfiguration redisStandaloneConfiguration = new RedisStandaloneConfiguration();
                if (!StringUtils.isEmpty(password)) {
                    redisStandaloneConfiguration.setPassword(RedisPassword.of(password));
                }
                redisStandaloneConfiguration.setPort(n.getPort());
                redisStandaloneConfiguration.setHostName(n.getHost());
                factory = new JedisConnectionFactory(redisStandaloneConfiguration, jedisClientConfiguration);
            }
        }
        // cluster
        else {
            log.info("[consistency framework] Redis use RedisStandaloneConfiguration");
            RedisClusterConfiguration redisClusterConfiguration = new RedisClusterConfiguration();
            nodes.forEach(n -> {
                redisClusterConfiguration.addClusterNode(new RedisNode(n.getHost(), n.getPort()));
            });
            if (!StringUtils.isEmpty(password)) {
                redisClusterConfiguration.setPassword(password);
            }
            redisClusterConfiguration.setMaxRedirects(redisConfigProperties.getMaxRedirect());
            factory = new JedisConnectionFactory(redisClusterConfiguration, jedisClientConfiguration);
        }
        return factory;
    }
    
    private void initRedisTemplate(RedisTemplate redisTemplate, JedisConnectionFactory factory) {
        Jackson2JsonRedisSerializer jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer(Object.class);
        ObjectMapper om = new ObjectMapper();
        // 指定要序列化的域，field,get和set,以及修饰符范围，ANY是都有包括private和public
        om.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        // 指定序列化输入的类型，类必须是非final修饰的，final修饰的类，比如String,Integer等会抛出异常
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        jackson2JsonRedisSerializer.setObjectMapper(om);
        // string 的序列化
        StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
        // key采用String的序列化方式
        redisTemplate.setKeySerializer(stringRedisSerializer);
        // hash的key也采用String的序列化方式
        redisTemplate.setHashKeySerializer(stringRedisSerializer);
        // value序列化方式采用jackson
        redisTemplate.setValueSerializer(jackson2JsonRedisSerializer);
        // hash的value序列化方式采用jackson
        redisTemplate.setHashValueSerializer(jackson2JsonRedisSerializer);
        //设置连接工厂
        redisTemplate.setConnectionFactory(factory);
    }
    
    /**
     * 配置检查
     *
     * @param consistencyParallelTaskConfigProperties 并行任务相关的配置
     * @param shardModeConfigProperties               分片模式相关配置
     */
    private void doConfigCheck(TendConsistencyParallelTaskConfigProperties consistencyParallelTaskConfigProperties,
            ShardModeConfigProperties shardModeConfigProperties) {
        if (!StringUtils.isEmpty(consistencyParallelTaskConfigProperties.getThreadPoolKeepAliveTimeUnit())) {
            try {
                TimeUnit.valueOf(consistencyParallelTaskConfigProperties.getThreadPoolKeepAliveTimeUnit());
            } catch (IllegalArgumentException e) {
                log.error("检查threadPoolKeepAliveTimeUnit配置时，发生异常", e);
                String errMsg = "threadPoolKeepAliveTimeUnit配置错误！注意：" +
                        "请在[SECONDS,MINUTES,HOURS,DAYS,NANOSECONDS,MICROSECONDS,MILLISECONDS]任选其中之一";
                throw new ConsistencyException(errMsg);
            }
        }
        
        if (!StringUtils.isEmpty(consistencyParallelTaskConfigProperties.getTaskScheduleTimeRangeClassName())) {
            // 校验是否存在该类
            Class<?> taskScheduleTimeRangeClass = ReflectTools.checkClassByName(
                    consistencyParallelTaskConfigProperties.getTaskScheduleTimeRangeClassName());
            if (ObjectUtils.isEmpty(taskScheduleTimeRangeClass)) {
                String errMsg = String.format("未找到 %s 类，请检查类路径是否正确",
                        consistencyParallelTaskConfigProperties.getTaskScheduleTimeRangeClassName());
                throw new ConsistencyException(errMsg);
            }
            // 用户自定义校验：校验是否实现了TaskTimeRangeQuery接口
            boolean result = ReflectTools.isRealizeTargetInterface(taskScheduleTimeRangeClass,
                    TaskTimeRangeQuery.class.getName());
            if (!result) {
                String errMsg = String.format("%s 类，未实现TaskTimeRangeQuery接口",
                        consistencyParallelTaskConfigProperties.getTaskScheduleTimeRangeClassName());
                throw new ConsistencyException(errMsg);
            }
        }
        
        if (!StringUtils.isEmpty(shardModeConfigProperties.getShardingKeyGeneratorClassName())) {
            // 校验是否存在该类
            Class<?> shardingKeyGeneratorClass = ReflectTools.checkClassByName(
                    shardModeConfigProperties.getShardingKeyGeneratorClassName());
            if (ObjectUtils.isEmpty(shardingKeyGeneratorClass)) {
                String errMsg = String.format("未找到 %s 类，请检查类路径是否正确",
                        shardModeConfigProperties.getShardingKeyGeneratorClassName());
                throw new ConsistencyException(errMsg);
            }
            // 用户自定义校验：校验是否实现了ShardingKeyGenerator接口
            boolean result = ReflectTools.isRealizeTargetInterface(shardingKeyGeneratorClass,
                    ShardingKeyGenerator.class.getName());
            if (!result) {
                String errMsg = String.format("%s 类，未实现ShardingKeyGenerator接口",
                        shardModeConfigProperties.getShardingKeyGeneratorClassName());
                throw new ConsistencyException(errMsg);
            }
        }
    }
    
    /**
     * zk配置检查
     *
     * @param curatorConfigProperties zk配置类
     */
    private void doCuratorConfigCheck(CuratorConfigProperties curatorConfigProperties) {
        if (StringUtils.isEmpty(curatorConfigProperties.getConnectString())) {
            String errMsg = String.format("zk配置类未配置zk地址：%s",
                    curatorConfigProperties.getConnectString());
            throw new ConsistencyException(errMsg);
        }
    }
    
    /**
     * redis配置检查
     *
     * @param redisConfigProperties redis配置类
     */
    private void doRedisConfigCheck(RedisConfigProperties redisConfigProperties) {
        if (StringUtils.isEmpty(redisConfigProperties.getNode()) || StringUtils.isEmpty(redisConfigProperties.getSentinel())) {
            String errMsg = "redis配置类未配置redis地址";
            throw new ConsistencyException(errMsg);
        }
    }
}
