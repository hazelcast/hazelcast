/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl.eventservice.impl;

import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.util.executor.StripedRunnable;
import com.hazelcast.util.executor.TimeoutRunnable;

import java.util.concurrent.TimeUnit;

public final class LocalEventDispatcher implements StripedRunnable, TimeoutRunnable {
    private EventServiceImpl eventService;
    private final String serviceName;
    private final Object event;
    private final Object listener;
    private final int orderKey;
    private final long timeoutMs;

    LocalEventDispatcher(EventServiceImpl eventService, String serviceName, Object event, Object listener,
                         int orderKey, long timeoutMs) {
        this.eventService = eventService;
        this.serviceName = serviceName;
        this.event = event;
        this.listener = listener;
        this.orderKey = orderKey;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public long getTimeout() {
        return timeoutMs;
    }

    @Override
    public TimeUnit getTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

    @Override
    public void run() {
        final EventPublishingService<Object, Object> service = eventService.nodeEngine.getService(serviceName);
        if (service == null) {
            if (eventService.nodeEngine.isActive()) {
                throw new IllegalArgumentException("Service[" + serviceName + "] could not be found!");
            }
            return;
        }

        service.dispatchEvent(event, listener);
    }

    @Override
    public int getKey() {
        return orderKey;
    }
}
