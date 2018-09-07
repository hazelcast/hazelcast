/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring.scheduler;

import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.ScheduledExecutorServiceProxy;
import com.hazelcast.spi.NodeEngine;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;

import java.util.Date;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Spring {@link TaskScheduler} implementation that delegates to Hazelcast
 * {@link com.hazelcast.scheduledexecutor.IScheduledExecutorService IScheduledExecutorService}.
 */
public class SpringTaskSchedulerProxy extends ScheduledExecutorServiceProxy implements TaskScheduler {

    public SpringTaskSchedulerProxy(String name, NodeEngine nodeEngine, DistributedScheduledExecutorService service) {
        super(name, nodeEngine, service);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable task, Trigger trigger) {
        return new ReschedulingRunnable(task, trigger, this).schedule();
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable task, Date startTime) {
        long delay = toDelayMilliSeconds(startTime);
        return super.schedule(task, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Date startTime, long period) {
        return super.scheduleAtFixedRate(task, toDelayMilliSeconds(startTime), period, TimeUnit.MILLISECONDS);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long period) {
        return super.scheduleAtFixedRate(task, 0, period, TimeUnit.MILLISECONDS);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Date startTime, long delay) {
        return scheduleAtFixedRate(task, startTime, delay);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, long delay) {
        return scheduleAtFixedRate(task, delay);
    }

    private static long toDelayMilliSeconds(Date startTime) {
        long startTimeMs = startTime.getTime();
        long now = System.currentTimeMillis();
        return startTimeMs - now;
    }
}
