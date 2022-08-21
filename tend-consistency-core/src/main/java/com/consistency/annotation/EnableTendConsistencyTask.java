package com.consistency.annotation;

import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 启用一致性框架
 *
 * @author wzw
 */
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Target(ElementType.TYPE)
@Import({ConsistencyTaskSelector.class, LoadBalanceRegister.class})
public @interface EnableTendConsistencyTask {

}
