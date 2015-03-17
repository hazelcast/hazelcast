package com.hazelcast.spi.impl.waitnotifyservice;

import com.hazelcast.spi.WaitNotifyService;

public interface InternalWaitNotifyService extends WaitNotifyService {

    void cancelWaitingOps(String serviceName, Object objectId, Throwable cause);
}
