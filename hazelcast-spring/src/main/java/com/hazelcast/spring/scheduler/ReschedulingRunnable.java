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

import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.support.SimpleTriggerContext;

import java.util.Date;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Adapted from {@link org.springframework.scheduling.concurrent.ReschedulingRunnable}.
 */
class ReschedulingRunnable implements Runnable, ScheduledFuture<Object> {

    private final Runnable delegate;

    private final Trigger trigger;

    private final SimpleTriggerContext triggerContext = new SimpleTriggerContext();

    private final IScheduledExecutorService scheduledExecutorService;

    private final Object triggerContextMonitor = new Object();

    private ScheduledFuture<?> currentFuture;

    private Date scheduledExecutionTime;

    ReschedulingRunnable(Runnable delegate, Trigger trigger, IScheduledExecutorService scheduledExecutorService) {
        this.delegate = delegate;
        this.trigger = trigger;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    ScheduledFuture<?> schedule() {
        synchronized (triggerContextMonitor) {
            scheduledExecutionTime = trigger.nextExecutionTime(triggerContext);
            if (scheduledExecutionTime == null) {
                return null;
            }
            long initialDelay = scheduledExecutionTime.getTime() - System.currentTimeMillis();
            currentFuture = scheduledExecutorService.schedule(this, initialDelay, TimeUnit.MILLISECONDS);
            return this;
        }
    }

    @Override
    public void run() {
        Date actualExecutionTime = new Date();
        delegate.run();
        Date completionTime = new Date();
        synchronized (triggerContextMonitor) {
            triggerContext.update(scheduledExecutionTime, actualExecutionTime, completionTime);
            if (!currentFuture.isCancelled()) {
                schedule();
            }
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        synchronized (triggerContextMonitor) {
            return currentFuture.cancel(mayInterruptIfRunning);
        }
    }

    @Override
    public boolean isCancelled() {
        synchronized (triggerContextMonitor) {
            return currentFuture.isCancelled();
        }
    }

    @Override
    public boolean isDone() {
        synchronized (triggerContextMonitor) {
            return currentFuture.isDone();
        }
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException {
        ScheduledFuture<?> curr;
        synchronized (triggerContextMonitor) {
            curr = currentFuture;
        }
        return curr.get();
    }

    @Override
    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        ScheduledFuture<?> curr;
        synchronized (triggerContextMonitor) {
            curr = currentFuture;
        }
        return curr.get(timeout, unit);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        ScheduledFuture<?> curr;
        synchronized (triggerContextMonitor) {
            curr = currentFuture;
        }
        return curr.getDelay(unit);
    }

    @Override
    @SuppressFBWarnings("EQ_COMPARETO_USE_OBJECT_EQUALS")
    public int compareTo(Delayed other) {
        if (this == other) {
            return 0;
        }
        long diff = getDelay(TimeUnit.MILLISECONDS) - other.getDelay(TimeUnit.MILLISECONDS);
        return (diff == 0 ? 0 : ((diff < 0) ? -1 : 1));
    }

}
