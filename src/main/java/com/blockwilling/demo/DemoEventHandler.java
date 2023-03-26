package com.blockwilling.demo;

import com.blockwilling.worker.disruptor.Event;
import com.blockwilling.worker.EventHandler;
import com.blockwilling.worker.EventQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Created by blockWilling  on 2022/8/1.
 */
@Slf4j
@Component
public class DemoEventHandler implements EventHandler<Event> {
    @Override
    public void onEvent(Event event, String type, EventQueue eventQueue) throws Exception {
        Object body =null;
        try {
            body=event.getBody();
//            eventQueue.fail(body.toString());
            eventQueue.suc(body.toString());
        } catch (Exception e) {
            log.error("【DemoEventHandler】",e);
            if(body!=null){
                eventQueue.fail(body.toString());
            }

        }

    }
}
