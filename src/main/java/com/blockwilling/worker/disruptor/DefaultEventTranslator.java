package com.blockwilling.worker.disruptor;

import com.lmax.disruptor.EventTranslatorTwoArg;

/**
 * 默认的事件属性填充器
 * Created by blockWilling  on 2022/8/1.
 */
public class DefaultEventTranslator implements EventTranslatorTwoArg<Event,String,Object> {
    @Override
    public void translateTo(Event event, long sequence, String arg0, Object arg1) {
        event.setEventType(arg0);
        event.setBody(arg1);
    }
}
