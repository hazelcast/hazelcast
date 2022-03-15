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

package com.hazelcast.scheduledexecutor.impl;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.lang.Thread.currentThread;

/**
 * Simple adapter that unwraps the Future delegation done in
 * {@link com.hazelcast.spi.impl.executionservice.impl.DelegatingCallableTaskDecorator} for single-run Callable tasks.
 *
 * @param <V>
 */
public class DelegatingScheduledFutureStripper<V>
        implements ScheduledFuture<V> {

    private final ScheduledFuture<V> original;

    public DelegatingScheduledFutureStripper(ScheduledFuture<V> original) {
        checkNotNull(original, "Original is null.");
        this.original = original;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return original.getDelay(unit);
    }

    @Override
    public int compareTo(Delayed o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean cancelled = original.cancel(mayInterruptIfRunning);
        try {
            return peel().cancel(mayInterruptIfRunning);
        } catch (CancellationException e) {
            // ignore; cancelled before scheduled-in
            ignore();
        }

        return cancelled;
    }

    @Override
    public boolean isCancelled() {
        return original.isCancelled() || peel().isCancelled();
    }

    @Override
    public boolean isDone() {
        return original.isDone() && peel().isDone();
    }

    @Override
    public V get()
            throws InterruptedException, ExecutionException {
        return peel().get();
    }

    @Override
    public V get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }

    private Future<V> peel() {
        // Get the delegating Executor Future (in case of Callable, see. DelegatingCallableTaskDecorator)
        try {
            return (Future) original.get();
        } catch (InterruptedException e) {
            currentThread().interrupt();
            sneakyThrow(e);
        } catch (ExecutionException e) {
            sneakyThrow(e);
        }

        return null;
    }

    private void ignore() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DelegatingScheduledFutureStripper<?> that = (DelegatingScheduledFutureStripper<?>) o;

        return original.equals(that.original);
    }

    @Override
    public int hashCode() {
        return original.hashCode();
    }
}
