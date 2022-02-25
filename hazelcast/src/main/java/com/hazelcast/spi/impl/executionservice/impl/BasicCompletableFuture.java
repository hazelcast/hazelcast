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

package com.hazelcast.spi.impl.executionservice.impl;

import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static java.lang.Thread.currentThread;

/**
 * Wraps a {@link Future} to make it a {@link InternalCompletableFuture}.
 * <p>
 * Ensures two-directional binding when it comes to cancellation:<ul>
 *     <li>if delegate future cancelled - this future may be done or cancelled
 *     <li>if this future cancelled - delegate future may be done or cancelled
 * </ul>
 * Ensures the transfer of the result from the delegate future to this future
 * on execution of {@link #get()} and {@link #isDone()} methods.
 */
class BasicCompletableFuture<V> extends InternalCompletableFuture<V> {

    final Future<V> delegate;

    BasicCompletableFuture(Future<V> delegate) {
        this.delegate = delegate;
    }

    @Override
    public V get()
            throws InterruptedException, ExecutionException {
        try {
            return get(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return (V) ensureResultSet(timeout, unit);
    }

    private Object ensureResultSet(long timeout, TimeUnit unit)
            throws ExecutionException, CancellationException, TimeoutException {
        Object result = null;
        try {
            result = delegate.get(timeout, unit);
        } catch (TimeoutException ex) {
            throw ex;
        } catch (InterruptedException ex) {
            currentThread().interrupt();
            sneakyThrow(ex);
        } catch (ExecutionException | CancellationException ex) {
            completeExceptionally(ex);
            throw ex;
        } catch (Throwable t) {
            result = t;
            completeExceptionally(t);
            sneakyThrow(t);
        }
        complete((V) result);
        return result;
    }

    @Override
    public boolean isDone() {
        if (delegate.isDone()) {
            try {
                ensureResultSet(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (ExecutionException | CancellationException | TimeoutException ignored) {
                ignore(ignored);
            }
            return true;
        } else {
            return super.isDone();
        }
    }

    @Override
    public boolean isCancelled() {
        if (delegate.isCancelled()) {
            if (!super.isCancelled()) {
                cancel(true);
            }
            return true;
        } else {
            return super.isCancelled();
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (!delegate.isCancelled()) {
            delegate.cancel(mayInterruptIfRunning);
        }
        return super.cancel(mayInterruptIfRunning);
    }
}
