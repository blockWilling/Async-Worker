package com.blockwilling.worker.disruptor;

import com.lmax.disruptor.EventFactory;

/**
 * 用于ringbuffer占位
 * Created by blockWilling  on 2022/8/1.
 */
public class DefaultEventFactory implements EventFactory<Event> {
    @Override
    public Event newInstance() {
        return new Event();
    }
}
