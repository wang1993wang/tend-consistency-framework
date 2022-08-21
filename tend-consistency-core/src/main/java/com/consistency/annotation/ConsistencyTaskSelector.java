package com.consistency.annotation;

import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;

/**
 * ComponentScanConfig类导入spring
 *
 * @author wzw
 */
public class ConsistencyTaskSelector implements ImportSelector {
    
    @Override
    public String[] selectImports(AnnotationMetadata annotationMetadata) {
        return new String[]{ComponentScanConfig.class.getName()};
    }
}
