/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
 * Reasoning is given tasks to {@link ScheduledThreadPoolExecutor} stops silently if there is an execution exception.
 *
 * Note: Task decoration is only needed to call given tasks {@code toString} methods.
 *
 * {@link java.util.concurrent.ScheduledExecutorService#scheduleWithFixedDelay}
 */
public class LoggingScheduledExecutor extends ScheduledThreadPoolExecutor {

    private final ILogger logger;
    private volatile boolean shutdownInitiated;

    public LoggingScheduledExecutor(ILogger logger, int corePoolSize, ThreadFactory threadFactory) {
        super(corePoolSize, threadFactory);
        this.logger = checkNotNull(logger, "logger cannot be null");
    }

    public LoggingScheduledExecutor(ILogger logger, int corePoolSize, ThreadFactory threadFactory,
                                    RejectedExecutionHandler handler) {
        super(corePoolSize, threadFactory, handler);
        this.logger = checkNotNull(logger, "logger cannot be null");
    }

    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
        return new LoggingDelegatingFuture<V>(runnable, task);
    }

    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(Callable<V> callable, RunnableScheduledFuture<V> task) {
        return new LoggingDelegatingFuture<V>(callable, task);
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
     * Only goal of this wrapping is to call given tasks {@code toString} method. Because
     * there is no straightforward way to reach it due to the internal wrapping done by
     * {@link ScheduledThreadPoolExecutor}
     *
     * {@link ScheduledThreadPoolExecutor#scheduleWithFixedDelay}
     */
    static class LoggingDelegatingFuture<V> implements RunnableScheduledFuture<V> {

        private final Object task;
        private final RunnableScheduledFuture<V> delegate;

        LoggingDelegatingFuture(Object task, RunnableScheduledFuture<V> delegate) {
            this.task = task;
            this.delegate = delegate;
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
            if (!(o instanceof LoggingDelegatingFuture)) {
                return false;
            }
            LoggingDelegatingFuture<?> that = (LoggingDelegatingFuture<?>) o;
            return delegate.equals(that.delegate);
        }

        @Override
        public int hashCode() {
            return delegate.hashCode();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return delegate.cancel(mayInterruptIfRunning);
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
            return "LoggingDelegatingFuture{task=" + task + '}';
        }
    }
}
