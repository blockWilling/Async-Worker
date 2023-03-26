package com.blockwilling.worker;


/**
 * redis待消费队列与本地队列的桥梁
 * Created by blockWilling  on 2022/8/1.
 */
public interface EventQueue {
    /**
     * 供{@link EventPublishThread}拉取任务
     * @return
     */
     Object next();

    /**
     * 成功
     * @param payload
     */
    void suc(String payload);

    /**
     * 失败
     * @param payload
     */
    void fail(String payload);

    /**
     * 重新入队等待队列
     * @param payload
     */
    void reEnqueue(String payload);

    /**
     * 队列基础名称
     * @return
     */
    String getName();
}
