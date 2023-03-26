package com.blockwilling.worker;

/**
 * Created by blockWilling  on 2022/8/1.
 */
@FunctionalInterface
public interface Cancelable {
    void cancel();
}
