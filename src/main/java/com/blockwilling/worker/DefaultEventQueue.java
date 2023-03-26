package com.blockwilling.worker;

import com.blockwilling.alert.AlertMaker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.lang.ref.WeakReference;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by blockWilling  on 2022/8/1.
 */
@Component
@Slf4j
public class DefaultEventQueue implements EventQueue,DisposableBean {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private AlertMaker alertMaker;
    private String localProcessingQueueName;
    private String failQueueName;
    private String name;
    private static final String FAIL_QUEUE_SUFFIX = "failed";
    private static final String QUEUE_NAME_LINK = "_";
    private static final Integer DEFAULT_WAIT_TIME = 5;
    private ReentrantLock lock;
    private Condition condition;
    private static final Integer DEFAULT_FAILED_TIMES = 3;

    private static final Long SUC_ERROR_FLAG = -1L;

    private static final Long FAIL_RETRY_ERROR_FLAG = -2L;

    private static final Long FAIL_CANCELED_ERROR_FLAG = -3L;
    //消息处理失败计数
    private LinkedHashMap<String, AtomicInteger> failedCountCache;
    //消息在本地处理队列的时长
    private LinkedHashMap<String, Long> processingDurationCache;

    private static final String MOVE_TO_QUEUE_LUA_SCRIPT = "if redis.call('lrem', KEYS[1],0,ARGV[1]) >0 then return redis.call('lpush', KEYS[2],ARGV[1]) else return -1 end";
    private DefaultRedisScript<Long> moveToQueueScript;

    public DefaultEventQueue() {
        this.name = getName();
        String ip = null;
        try {
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            log.error("【DemoEventQueue】constructor", e);
        }
        this.localProcessingQueueName = name + QUEUE_NAME_LINK + ip;
        this.failQueueName = name + QUEUE_NAME_LINK + FAIL_QUEUE_SUFFIX;
        lock = new ReentrantLock();
        condition = lock.newCondition();
       /* failedCountCache = CacheBuilder
                .newBuilder().build(new CacheLoader<String, AtomicInteger>() {
                    @Override
                    public AtomicInteger load(String key) throws Exception {
                        return new AtomicInteger(0);
                    }
                });
        processingDurationCache = CacheBuilder
                .newBuilder().build(new CacheLoader<String, Long>() {
                    @Override
                    public Long load(String key) throws Exception {
                        return 0L;
                    }
                });*/
        failedCountCache = new LinkedHashMap<String, AtomicInteger>();
        processingDurationCache = new LinkedHashMap<String, Long>();
        moveToQueueScript = new DefaultRedisScript<Long>(MOVE_TO_QUEUE_LUA_SCRIPT, Long.class);
    }


    @Override
    public Object next() {
        while (true) {
            String payload = null;
            try {
                payload = stringRedisTemplate.opsForList().rightPopAndLeftPush(name, localProcessingQueueName);
                if (!StringUtils.isEmpty(payload)) {
                    Long aLong = processingDurationCache.get(payload);
                    if (aLong == null || aLong <= 0) {
                        processingDurationCache.put(payload, System.currentTimeMillis());
                    }
                }
            } catch (Exception e) {
                log.error("【"+this.getName()+"】next-pop&push", e);
                //重新入队等待队列
                alertMaker.alert("【"+this.getName()+"】next-pop&push,payload:" + payload, e);
                if (!StringUtils.isEmpty(payload)) {
                    try {
                        stringRedisTemplate.opsForList().leftPush(name, payload);
                    } catch (Exception e1) {
                        alertMaker.alert("【"+this.getName()+"】next-re-enqueue-failed,payload:" + payload + "queueName:" + name, e);
                    }
                }
                continue;
            }
            if (!StringUtils.isEmpty(payload)) {
                return payload;
            }
            lock.lock();
            try {
                condition.await(DEFAULT_WAIT_TIME, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("【DemoEventQueue】next-wait", e);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void suc(String payload) {
        try {
            processingDurationCache.put(payload, System.currentTimeMillis());
            Long remove = stringRedisTemplate.opsForList().remove(localProcessingQueueName, 0, payload);
            if (remove.equals(0L)) {
                log.error("【"+this.getName()+"】localProcessingQueue may not exist this payload ,please check!;param=execute-ret:{},payload:{},source_queue:{}", remove, payload, localProcessingQueueName);
            }
        } catch (Exception e) {
            alertMaker.alert("【"+this.getName()+"】suc,payload:" + payload, e);
            log.error("【"+this.getName()+"】suc", e);
            processingDurationCache.put(payload, SUC_ERROR_FLAG);
        }
        processingDurationCache.remove(payload);
    }

    @Override
    public void fail(String payload) {
        try {
            processingDurationCache.put(payload, System.currentTimeMillis());
            AtomicInteger atomicInteger = failedCountCache.getOrDefault(payload, new AtomicInteger(0));
            if (atomicInteger.getAndIncrement() < DEFAULT_FAILED_TIMES) {
                try {
                    failedCountCache.put(payload,atomicInteger);
                    Long execute = stringRedisTemplate.execute(moveToQueueScript, Arrays.asList(localProcessingQueueName, name), payload);
                    handleScriptRet(payload, execute, localProcessingQueueName, name);
                } catch (Exception e) {
                    alertMaker.alert("【"+this.getName()+"】fail-retry,payload:" + payload, e);
                    log.error("【"+this.getName()+"】fail-retry", e);
                    processingDurationCache.put(payload, FAIL_RETRY_ERROR_FLAG);
                }
            } else {
                failedCountCache.remove(payload);
                alertMaker.alert("【"+this.getName()+"】fail-overRetry,payload:" + payload, null);
                Long execute = stringRedisTemplate.execute(moveToQueueScript, Arrays.asList(localProcessingQueueName, failQueueName), payload);
                handleScriptRet(payload, execute, localProcessingQueueName, failQueueName);
            }
        } catch (Exception e) {
            failedCountCache.remove(payload);
            log.error("【"+this.getName()+"】fail-ex", e);
            try {
                alertMaker.alert("【"+this.getName()+"】fail-ex,payload:" + payload, e);
                Long execute = stringRedisTemplate.execute(moveToQueueScript, Arrays.asList(localProcessingQueueName, failQueueName), payload);
                handleScriptRet(payload, execute, localProcessingQueueName, failQueueName);
            } catch (Exception e1) {
                processingDurationCache.put(payload, FAIL_CANCELED_ERROR_FLAG);
            }
        }
        processingDurationCache.remove(payload);

    }

    @Override
    public void reEnqueue(String payload) {
        try {
            Long execute = stringRedisTemplate.execute(moveToQueueScript, Arrays.asList(localProcessingQueueName, name), payload);
            handleScriptRet(payload, execute, localProcessingQueueName, name);
        } catch (Exception e) {
            alertMaker.alert("【"+this.getName()+"】reEnqueue,payload:" + payload, e);
            log.error("【"+this.getName()+"】reEnqueue", e);
        }
    }

    @Override
    public String getName() {
        return "queue_default";
    }

    @Scheduled(fixedRate = 30 * 1000)
    public void execute4Timeout() {
        long now = System.currentTimeMillis();
        Iterator<String> iterator = processingDurationCache.keySet().iterator();
        while (iterator.hasNext()) {
            String s = iterator.next();
            try {
                Long unchecked = processingDurationCache.get(s);
                if (unchecked == null) {
                    continue;
                }

                //补偿suc删除本地队列失败，fail处理失败
                if (SUC_ERROR_FLAG.equals(unchecked)) {
                    Long remove = stringRedisTemplate.opsForList().remove(localProcessingQueueName, 0, s);
                    if (remove.equals(0L)) {
                        log.error("【"+this.getName()+"】localProcessingQueue may not exist this payload ,please check!;param=execute-ret:{},payload:{},source_queue:{}", remove, s, localProcessingQueueName);
                    }
                } else if (FAIL_CANCELED_ERROR_FLAG.equals(unchecked)) {
                    Long execute = stringRedisTemplate.execute(moveToQueueScript, Arrays.asList(localProcessingQueueName, failQueueName), s);
                    handleScriptRet(s, execute, localProcessingQueueName, name);
                } else if (FAIL_RETRY_ERROR_FLAG.equals(unchecked) || (now - unchecked > 30 * 1000)) {
                    Long execute = stringRedisTemplate.execute(moveToQueueScript, Arrays.asList(localProcessingQueueName, name), s);
                    handleScriptRet(s, execute, localProcessingQueueName, name);
                } else {
                    continue;
                }
                //补偿或者超时处理完成
                iterator.remove();
            } catch (Exception e) {
                processingDurationCache.put(s, System.currentTimeMillis());
                alertMaker.alert("【"+this.getName()+"】execute4Timeout-ex,payload:" + s, e);
                log.error("【"+this.getName()+"】execute4Timeout-ex", e);
                continue;
            }
        }
    }

    /**
     * payload可能失败重试路由到了其他节点，清理当前节点失败计数缓存，因为可能会有再次失败路由回来的可能，所以增长调度任务执行周期
     * 就算删除了，也就是多重试
     */
    @Scheduled(fixedRate = 60 * 1000)
    public void execute4CacheClean() {
        Iterator<String> iterator = failedCountCache.keySet().iterator();
        while (iterator.hasNext()) {
            String next = iterator.next();
            Long aLong = processingDurationCache.get(next);
            long now = System.currentTimeMillis();
            if (aLong != null && aLong <= 0) {
                continue;
            }
            if (aLong == null || (now - aLong > 30 * 1000)) {
                iterator.remove();
            }
        }
    }

    private void handleScriptRet(String s, Long execute, String sourceQueue, String destQueue) {
        if (execute.equals(-1L)) {
            log.error("【"+this.getName()+"】localProcessingQueue may not exist this payload ,please check!;param=execute-ret:{},payload:{},source_queue:{},dest_queue:{}", execute, s, sourceQueue, destQueue);
        } else if (execute.equals(0L)) {
            log.error("【"+this.getName()+"】maybe enqueue failed,please check!;param=execute-ret:{},payload:{},source_queue:{},dest_queue:{}", execute, s, sourceQueue, destQueue);
        }
    }

    @Override
    public void destroy() throws Exception {
        failedCountCache.clear();
        processingDurationCache.clear();
    }

    /**
     * 暂时不使用弱引用包裹（当前类单例，且被spring final map 强引用关联，随进程结束而销毁），代码控制。如果后续发现有大量类似缓存，可以考虑
     * @param <T>
     */
    static class Entry<T> extends WeakReference<T> {
        Object value;

        Entry(T value) {
            super(value);
            this.value = value;
        }
    }
    <T, V> void wrappedCachePut(LinkedHashMap<Entry<T>, V> cache,T t,V v){
        if(cache.containsKey(t)){

        }else {
            cache.put(new Entry<>(t),v);
        }
    }
}
