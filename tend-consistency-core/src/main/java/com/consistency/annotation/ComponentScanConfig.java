package com.consistency.annotation;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * bean扫描
 *
 * @author wzw
 */
@Configuration
@ComponentScan(value = {"com.consistency"})
@MapperScan(basePackages = {"com.consistency"})
public interface ComponentScanConfig {

}
