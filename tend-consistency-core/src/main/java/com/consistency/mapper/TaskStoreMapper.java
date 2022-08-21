package com.consistency.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.consistency.model.ConsistencyTaskInstance;
import org.springframework.stereotype.Repository;

/**
 * 任务表mapper
 *
 * @author wzw
 */
@Repository
public interface TaskStoreMapper extends BaseMapper<ConsistencyTaskInstance> {

}
