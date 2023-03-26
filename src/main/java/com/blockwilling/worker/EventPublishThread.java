package com.blockwilling.worker;

import com.blockwilling.worker.disruptor.DefaultEventTranslator;
import com.lmax.disruptor.RingBuffer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 本地队列与disruptor内存队列的桥梁
 * Created by blockWilling  on 2022/8/1.
 */
@Data
@Slf4j
public class EventPublishThread implements Callable, Cancelable {
    String eventType;
    EventQueue eventQueue;
    RingBuffer ringBuffer;
    AtomicBoolean isRunning = new AtomicBoolean(true);
    DefaultEventTranslator defaultEventTranslator=new DefaultEventTranslator();

    public EventPublishThread(String eventType, EventQueue eventQueue, RingBuffer ringBuffer) {
        this.eventType = eventType;
        this.eventQueue = eventQueue;
        this.ringBuffer = ringBuffer;
    }


    @Override
    public Object call() throws Exception {
        while (isRunning.get()) {
            Object next=null;
            try {
                if(next==null){
                    next=eventQueue.next();
                }
                if(next!=null){
                    ringBuffer.publishEvent(defaultEventTranslator,eventType,next);
                }
            } catch (Exception e) {
                //todo EventQueue会处理自己的异常，所以暂时这里不处理异常
                log.error("【EventPublishThread】",e);
            }
        }
        return null;
    }

    @Override
    public void cancel() {
        isRunning.compareAndSet(true, false);
    }
}
