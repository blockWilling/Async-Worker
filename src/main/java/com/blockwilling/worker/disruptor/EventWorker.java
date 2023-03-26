package com.blockwilling.worker.disruptor;

import com.blockwilling.worker.EventHandler;
import com.blockwilling.worker.EventPublishThread;
import com.blockwilling.worker.EventQueue;
import cn.hutool.core.map.MapUtil;
import com.google.common.collect.Lists;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by blockWilling  on 2022/8/1.
 */
@Component
public class EventWorker implements InitializingBean,DisposableBean {
    private Disruptor disruptor;
    private RingBuffer ringBuffer;
    private Map<EventQueue, EventHandler> eventHandlerMap;
    private Map<String, EventQueue> eventQueueMap;
    private ThreadPoolExecutor publishExecutor;
    private BlockingDeque<Runnable> publishQueue;
    private ThreadPoolExecutor workerExecutor;
    private BlockingDeque<Runnable> workerQueue;

    @Value("${asyncWorker.ringBufferSize}")
    Integer ringBufferSize;

    @Value("${asyncWorker.workerSize}")
    Integer workerSize;
    @Autowired
    ExceptionHandler exceptionHandler;

    private List<EventPublishThread> eventPublishThreadList;

    @Autowired
    public EventWorker(EventWorkerConfig config) {
        this.eventHandlerMap = config.getEventHandlerMap();
        if (!CollectionUtils.isEmpty(eventHandlerMap)) {
            int size = this.eventHandlerMap.size();
            this.eventQueueMap= MapUtil.newHashMap(size);
            for (Map.Entry<EventQueue, EventHandler> entry : eventHandlerMap.entrySet()) {
                EventQueue key = entry.getKey();
                this.eventQueueMap.put(key.getName(),key);
            }
            initPublishers(size);
        }

    }

    private void initPublishers(int size) {
        publishQueue = new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE);
        publishExecutor = new ThreadPoolExecutor(
                size, size,
                0, TimeUnit.SECONDS,
                publishQueue);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        initWorkers();
        initDisruptor();
        startPublishers();

    }

    private void startPublishers() {
        eventPublishThreadList= Lists.newArrayList();
        for (Map.Entry<String, EventQueue> stringEventQueueEntry : eventQueueMap.entrySet()) {
            String key = stringEventQueueEntry.getKey();
            EventQueue value = stringEventQueueEntry.getValue();
            EventPublishThread eventPublishThread = new EventPublishThread(key, value, ringBuffer);
            eventPublishThreadList.add(eventPublishThread);
            publishExecutor.submit(eventPublishThread);
        }
    }

    private void initDisruptor() {
        disruptor=new Disruptor<Event>(new DefaultEventFactory(),ringBufferSize,workerExecutor, ProducerType.MULTI,new BlockingWaitStrategy());
        ringBuffer = disruptor.getRingBuffer();
        disruptor.handleExceptionsWith(exceptionHandler);

        WorkHandler<Event> workHandler=new WorkHandler<Event>() {
            @Override
            public void onEvent(Event event) throws Exception {
                String eventType = event.getEventType();
                EventQueue eventQueue = eventQueueMap.get(eventType);
                EventHandler eventHandler = eventHandlerMap.get(eventQueue);
                eventHandler.onEvent(event,event.getEventType(),eventQueue);
            }
        };
        WorkHandler[] workHandlers=new WorkHandler[workerSize];
        for (int i = 0; i < workerSize; i++) {
            workHandlers[i]=workHandler;
        }
        disruptor.handleEventsWithWorkerPool(workHandlers);
        disruptor.start();
    }

    private void initWorkers() {
        workerQueue = new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE);
        workerExecutor = new ThreadPoolExecutor(
                workerSize, workerSize,
                0, TimeUnit.SECONDS,
                workerQueue);
    }

    @Override
    public void destroy() throws Exception {
        for (EventPublishThread eventPublishThread : eventPublishThreadList) {
            eventPublishThread.cancel();
        }
        publishExecutor.shutdown();
        workerExecutor.shutdown();
        disruptor.shutdown();
    }
}
