package com.blockwilling.worker.disruptor;

import com.blockwilling.worker.DefaultEventQueue;
import com.blockwilling.demo.DemoEventHandler;
import com.blockwilling.worker.EventHandler;
import com.blockwilling.worker.EventQueue;
import cn.hutool.core.map.MapUtil;
import lombok.Data;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Created by blockWilling  on 2022/8/1.
 */
@Component
@Data
public class EventWorkerConfig implements InitializingBean{
    @Autowired
    DefaultEventQueue defaultEventQueue;

    @Autowired
    DemoEventHandler demoEventHandler;

    private Map<EventQueue, EventHandler> eventHandlerMap;

    @Override
    public void afterPropertiesSet() throws Exception {
        eventHandlerMap= MapUtil.newHashMap();
        eventHandlerMap.put(defaultEventQueue,demoEventHandler);
    }
}
