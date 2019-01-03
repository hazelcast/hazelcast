/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.executor;

import com.hazelcast.logging.ILogger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.Thread.currentThread;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.SEVERE;

/**
 * Logs execution exceptions by overriding {@link ScheduledThreadPoolExecutor#afterExecute}
 * and {@link ScheduledThreadPoolExecutor#decorateTask} methods.
 *
 * Reasoning is given tasks to {@link ScheduledThreadPoolExecutor} stops silently if there is an
 * execution exception.
 *
 * Note: Task decoration is only needed to call given tasks {@code toString} methods.
 *
 * {@link java.util.concurrent.ScheduledExecutorService#scheduleWithFixedDelay}
 *
 * Remove on cancel:
 * To prevent running into an accumulation of cancelled task which can cause gc related problems
 * (e.g. transactions in combination with LockResource eviction), it is best that tasks are
 * removed from the scheduler on cancellation.
 *
 * In Java 7+ the there is a method {@code ScheduledThreadPoolExecutor#setRemoveOnCancelPolicy(boolean)}
 * which removes the runnable on cancellation. Removal of tasks is done in logarithmic time
 * (see ScheduledThreadPoolExecutor.DelayedWorkQueue.indexOf where there is a direct lookup
 * instead of a linear scan over the queue). So in Java 7 we try to set this method using
 * reflection. In Java 7+ the manualRemoveOnCancel is ignored, since it has an efficient removal
 * of cancelled tasks and therefore it will always apply it. If we would wrap the task with the
 * RemoveOnCancelFuture, it would even prevent constant time removal.
 *
 * In Java 6 there is no such method setRemoveOnCancelPolicy and therefore the task is
 * wrapped in a RemoveOnCancelFuture which will remove itself from the work-queue
 * on cancellation. However this removal isn't done in constant time, but in linear time
 * because Java 6 ScheduledThreadPoolExecutor doesn't support a constant time removal.
 *
 * By default in Java 6, the removal of this task is done based on the 'removeOnCancel'
 * property which gets its value from the
 * {@link com.hazelcast.spi.properties.GroupProperty#TASK_SCHEDULER_REMOVE_ON_CANCEL}
 */
public class LoggingScheduledExecutor extends ScheduledThreadPoolExecutor {

    // this property is accessible for for testing purposes.
    boolean manualRemoveOnCancel;
    private final ILogger logger;
    private volatile boolean shutdownInitiated;

    public LoggingScheduledExecutor(ILogger logger, int corePoolSize, ThreadFactory threadFactory) {
        this(logger, corePoolSize, threadFactory, false);
    }

    public LoggingScheduledExecutor(ILogger logger, int corePoolSize, ThreadFactory threadFactory,
                                    boolean removeOnCancel) {
        super(corePoolSize, threadFactory);
        this.logger = checkNotNull(logger, "logger cannot be null");
        this.manualRemoveOnCancel = manualRemoveOnCancel(removeOnCancel);
    }

    public LoggingScheduledExecutor(ILogger logger, int corePoolSize, ThreadFactory threadFactory,
                                    RejectedExecutionHandler handler) {
        this(logger, corePoolSize, threadFactory, false, handler);
    }

    public LoggingScheduledExecutor(ILogger logger, int corePoolSize, ThreadFactory threadFactory,
                                    boolean removeOnCancel,
                                    RejectedExecutionHandler handler) {
        super(corePoolSize, threadFactory, handler);
        this.logger = checkNotNull(logger, "logger cannot be null");
        this.manualRemoveOnCancel = manualRemoveOnCancel(removeOnCancel);
    }

    private boolean manualRemoveOnCancel(boolean removeOnCancel) {
        if (trySetRemoveOnCancelPolicy()) {
            return false;
        } else {
            return removeOnCancel;
        }
    }

    @SuppressFBWarnings("REC_CATCH_EXCEPTION")
    private boolean trySetRemoveOnCancelPolicy() {
        try {
            Method method = ScheduledThreadPoolExecutor.class.getMethod("setRemoveOnCancelPolicy", Boolean.TYPE);
            method.invoke(this, true);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
        if (!manualRemoveOnCancel) {
            return super.decorateTask(runnable, task);
        }

        return new RemoveOnCancelFuture<V>(runnable, task, this);
    }

    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(Callable<V> callable, RunnableScheduledFuture<V> task) {
        if (!manualRemoveOnCancel) {
            return super.decorateTask(callable, task);
        }

        return new RemoveOnCancelFuture<V>(callable, task, this);
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

    /**
     * The only goal of this task is to enable removal of the task from the executor
     * when the future is cancelled.
     */
    static class RemoveOnCancelFuture<V> implements RunnableScheduledFuture<V> {

        private final Object task;
        private final RunnableScheduledFuture<V> delegate;
        private final LoggingScheduledExecutor executor;

        RemoveOnCancelFuture(Object task, RunnableScheduledFuture<V> delegate, LoggingScheduledExecutor executor) {
            this.task = task;
            this.delegate = delegate;
            this.executor = executor;
        }

        @Override
        public boolean isPeriodic() {
            return delegate.isPeriodic();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return delegate.getDelay(unit);
        }

        @Override
        public void run() {
            delegate.run();
        }

        @Override
        public int compareTo(Delayed o) {
            return delegate.compareTo(o);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RemoveOnCancelFuture)) {
                return false;
            }
            RemoveOnCancelFuture<?> that = (RemoveOnCancelFuture<?>) o;
            return delegate.equals(that.delegate);
        }

        @Override
        public int hashCode() {
            return delegate.hashCode();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean removeOnCancel = !executor.isShutdown();
            boolean cancelled = delegate.cancel(mayInterruptIfRunning);
            if (cancelled && removeOnCancel) {
                executor.remove(this);
            }

            return cancelled;
        }

        @Override
        public boolean isCancelled() {
            return delegate.isCancelled();
        }

        @Override
        public boolean isDone() {
            return delegate.isDone();
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            return delegate.get();
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return delegate.get(timeout, unit);
        }

        @Override
        public String toString() {
            return "RemoveOnCancelFuture{task=" + task + '}';
        }
    }
}
