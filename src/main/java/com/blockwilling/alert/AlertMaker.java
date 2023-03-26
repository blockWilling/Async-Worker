package com.blockwilling.alert;

/**
 * Created by blockWilling  on 2022/8/2.
 */
@FunctionalInterface
public interface AlertMaker {
    void alert(String msg, Exception ex);
}
