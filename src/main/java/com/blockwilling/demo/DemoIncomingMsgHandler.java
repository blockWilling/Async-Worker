package com.blockwilling.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.Collections;

/**
 * Created by blockWilling  on 2022/8/1.
 */
@RestController
@RequestMapping("/worker")
public class DemoIncomingMsgHandler {
    @Autowired
    StringRedisTemplate stringRedisTemplate;
    @GetMapping("/produce")
    public String produce(int a) {
        /**
         * 如果是在redis cluster模式下，脚本操作多个key有可能有问题
         */
        final String ENQUEUE_LUA_SCRIPT = "local vals =ARGV\n" +
                "local len=redis.call('llen', KEYS[1])\n" +
                "if len < tonumber(KEYS[2])\n" +
                "\tthen for k,v in ipairs(vals) do\n" +
                "\t\tredis.call('lpush', KEYS[1],v)\n" +
                "\tend\n" +
                "\treturn len\n" +
                "else\n" +
                "\treturn -1\n" +
                "end";
        DefaultRedisScript<Long> enQueueScript= new DefaultRedisScript<Long>(ENQUEUE_LUA_SCRIPT, Long.class);;
        //这里一般接收mq消息开始
        //然后写redis，如果这里需要回溯任务(比如发现代码bug，或者有即时的新需求等)，那么写等待队列的同时需要写备份队列，就需要自己写lua脚本实现
//        stringRedisTemplate.opsForList().leftPush("queue_default",a+"");
        Long execute = stringRedisTemplate.execute(enQueueScript, Arrays.asList("queue_default","10"),"1","2","3");

        return "suc";
    }
}
