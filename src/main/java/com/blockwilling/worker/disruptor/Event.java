package com.blockwilling.worker.disruptor;

import lombok.Data;

/**
 * Created by blockWilling  on 2022/8/1.
 */
@Data
public class Event {
    String eventType;
    Object body;
}
