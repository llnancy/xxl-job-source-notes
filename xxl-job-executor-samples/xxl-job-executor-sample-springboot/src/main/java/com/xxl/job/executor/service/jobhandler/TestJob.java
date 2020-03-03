package com.xxl.job.executor.service.jobhandler;

import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHandler;
import org.springframework.stereotype.Component;

/**
 * @author sunchaser
 * @date 2020/3/3
 * @description
 * @since 1.0
 */
@JobHandler(value = "jobTest")
@Component
public class TestJob extends IJobHandler {
    @Override
    public ReturnT<String> execute(String param) throws Exception {
        System.out.println(111);
        return ReturnT.SUCCESS;
    }
}
