/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.executor;

import com.hazelcast.logging.ILogger;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.lang.Thread.currentThread;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.SEVERE;

/**
 * Logs execution exceptions by overriding {@link ScheduledThreadPoolExecutor#afterExecute}
 * and {@link ScheduledThreadPoolExecutor#decorateTask} methods.
 * <p>
 * Reasoning is given tasks to {@link ScheduledThreadPoolExecutor} stops silently if there is an
 * execution exception.
 * <p>
 * Note: Task decoration is only needed to call given tasks {@code toString} methods.
 * <p>
 * {@link java.util.concurrent.ScheduledExecutorService#scheduleWithFixedDelay}
 * <p>
 * Remove on cancel:
 * To prevent running into an accumulation of cancelled task which can cause gc related problems
 * (e.g. transactions in combination with LockResource eviction), it is best that tasks are
 * removed from the scheduler on cancellation.
 * <p>
 * In Java 7+ the there is a method {@code ScheduledThreadPoolExecutor#setRemoveOnCancelPolicy(boolean)}
 * which removes the runnable on cancellation. Removal of tasks is done in logarithmic time
 * (see ScheduledThreadPoolExecutor.DelayedWorkQueue.indexOf where there is a direct lookup
 * instead of a linear scan over the queue).
 */
public class LoggingScheduledExecutor extends ScheduledThreadPoolExecutor {

    private final ILogger logger;
    private volatile boolean shutdownInitiated;

    public LoggingScheduledExecutor(ILogger logger, int corePoolSize, ThreadFactory threadFactory) {
        super(corePoolSize, threadFactory);
        this.logger = checkNotNull(logger, "logger cannot be null");
        setRemoveOnCancelPolicy(true);
    }

    public LoggingScheduledExecutor(ILogger logger, int corePoolSize, ThreadFactory threadFactory,
                                    RejectedExecutionHandler handler) {
        super(corePoolSize, threadFactory, handler);
        this.logger = checkNotNull(logger, "logger cannot be null");
        setRemoveOnCancelPolicy(true);
    }

    @Override
    protected void afterExecute(Runnable runnable, Throwable throwable) {
        super.afterExecute(runnable, throwable);

        Level level = FINE;
        if (throwable == null && runnable instanceof ScheduledFuture && ((ScheduledFuture) runnable).isDone()) {
            try {
                ((Future) runnable).get();
            } catch (CancellationException ce) {
                throwable = ce;
            } catch (ExecutionException ee) {
                level = SEVERE;
                throwable = ee.getCause();
            } catch (InterruptedException ie) {
                throwable = ie;
                currentThread().interrupt();
            }
        }

        if (throwable instanceof RejectedExecutionException && shutdownInitiated) {
            level = Level.FINE;
        }

        if (throwable != null) {
            logger.log(level, "Failed to execute " + runnable, throwable);
        }
    }

    public void notifyShutdownInitiated() {
        shutdownInitiated = true;
    }
}
