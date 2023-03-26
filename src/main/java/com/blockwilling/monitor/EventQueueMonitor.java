package com.blockwilling.monitor;

import com.blockwilling.worker.EventQueue;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Created by blockWilling  on 2022/8/16.
 */
@RestController
@RequestMapping("/monitor")
@Slf4j
public class EventQueueMonitor implements InitializingBean{

    @Autowired
    List<EventQueue> eventQueues;

    @Autowired
    StringRedisTemplate stringRedisTemplate;
    String ip = null;

    private static final String QUEUE_NAME_LINK = "_";

    @GetMapping("/overall")
    public List<MonitorVo> overall() {
        List<MonitorVo> monitorVos = new LinkedList<>();
        ListOperations<String, String> stringStringListOperations = stringRedisTemplate.opsForList();
        for (EventQueue eventQueue : eventQueues) {
            MonitorVo monitorVo = new MonitorVo();
            LinkedList<AbstractMap.SimpleEntry<String,Long>> objects = new LinkedList<>();
            String name = eventQueue.getName();
            Long size = stringStringListOperations.size(name);
            objects.add(new AbstractMap.SimpleEntry<String,Long>(name,size));
            Set<String> keys = stringRedisTemplate.keys(name+QUEUE_NAME_LINK + "*");
            for (String key : keys) {
                objects.add(new AbstractMap.SimpleEntry<String,Long>(key,stringStringListOperations.size(key)));
            }
            monitorVo.setList(objects);
            monitorVos.add(monitorVo);
        }
        return monitorVos;
    }
    @GetMapping("/detail")
    public List<String> detail(String name) {
        return stringRedisTemplate.opsForList().range(name,0,100);

    }

    @Override
    public void afterPropertiesSet() throws Exception {

        try {
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            log.error("【DemoEventQueue】constructor", e);
        }

    }

    @Data
    public static class MonitorVo {
        List<AbstractMap.SimpleEntry<String,Long>> list;

    }
}
