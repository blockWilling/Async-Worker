package com.blockwilling.worker.disruptor;

import com.alibaba.fastjson.JSON;
import com.lmax.disruptor.ExceptionHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Created by blockWilling  on 2022/8/2.
 */
@Component
@Slf4j
public class DefaultExceptionHandler implements ExceptionHandler {
    @Override
    public void handleEventException(Throwable ex, long sequence, Object event) {
            log.error("【DefaultExceptionHandler】handleEventException,event:{}", JSON.toJSONString(event),ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        log.error("【DefaultExceptionHandler】handleOnStartException",ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        log.error("【DefaultExceptionHandler】handleOnShutdownException",ex);
    }
}
