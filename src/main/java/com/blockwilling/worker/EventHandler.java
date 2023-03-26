package com.blockwilling.worker;

/**
 * 核心业务处理
 * Created by blockWilling  on 2022/8/1.
 */
public interface EventHandler<T> {
    void onEvent(T event, String type, EventQueue eventQueue) throws Exception;
}
