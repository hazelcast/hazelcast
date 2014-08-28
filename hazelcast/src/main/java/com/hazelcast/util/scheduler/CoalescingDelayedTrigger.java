/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.util.scheduler;


import com.hazelcast.spi.ExecutionService;
import com.hazelcast.util.Clock;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Coalescing trigger can be used when you want coalesce multiple execution of {@link java.lang.Runnable}.
 * Once you call the {@link #executeWithDelay()} it will wait specified amount of time before actually executing
 * the Runnable. If {@link #executeWithDelay()} is invoked again within the interval then the Runnable will
 * be executed only once.
 *
 * It also guarantees no execution will be delayed more than specified maximum delay.
 *
 * This class is not thread-safe and external synchronization must be ensured when multiple
 * threads are calling {@link #executeWithDelay()}
 *
 */
public class CoalescingDelayedTrigger {

    private final ExecutionService executionService;
    private final long delay;
    private final long maxDelay;
    private final Runnable runnable;

    private long hardLimit;
    private ScheduledFuture<?> future;

    /**
     * @param executionService
     * @param delay execution delay in ms
     * @param maxDelay maximum delay in ms. No action will be delayed more specified number of ms.
     * @param runnable action to be executed
     */
    public CoalescingDelayedTrigger(ExecutionService executionService, long delay, long maxDelay, Runnable runnable) {
        if (delay <= 0) {
            throw new IllegalArgumentException("Delay must be a positive number. Delay: " + delay);
        }
        if (maxDelay < delay) {
            throw new IllegalArgumentException("Maximum delay must be greater or equal than delay. "
                    + "Maximum delay: " + maxDelay + ", Delay: " + delay);
        }
        if (runnable == null) {
            throw new IllegalArgumentException("Runnable cannot be null");
        }

        this.executionService  = executionService;
        this.delay = delay;
        this.maxDelay = maxDelay;
        this.runnable = runnable;
    }

    /**
     * invoke delayed execution.
     *
     */
    public void executeWithDelay() {
        long now = Clock.currentTimeMillis();
        if (delay + now > hardLimit) {
            scheduleNewExecution(now);
        } else if (!tryPostponeExecution()) {
            scheduleNewExecution(now);
        }
    }

    private boolean tryPostponeExecution() {
        boolean cancel = future.cancel(false);
        if (!cancel) {
            return false;
        } else {
            future = executionService.schedule(runnable, delay, TimeUnit.MILLISECONDS);
            return true;
        }
    }

    private void scheduleNewExecution(long now) {
        future = executionService.schedule(runnable, delay, TimeUnit.MILLISECONDS);
        hardLimit = now + maxDelay;
    }
}
