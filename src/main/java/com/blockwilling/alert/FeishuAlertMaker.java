package com.blockwilling.alert;

import org.springframework.stereotype.Component;

/**
 * Created by blockWilling  on 2022/8/2.
 */
@Component
public class FeishuAlertMaker implements AlertMaker {
    @Override
    public void alert(String msg, Exception ex) {
        //todo 告警
    }
}
