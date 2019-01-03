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

package com.hazelcast.internal.util;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.spi.InternalCompletableFuture;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ExceptionUtil.sneakyThrow;

/**
 * Generic implementation of a completed {@link InternalCompletableFuture}.
 */
public class SimpleCompletedFuture<E> implements InternalCompletableFuture<E> {

    private final Object result;

    /**
     * Creates a completed future with the given result. The result can be
     * null.
     *
     * <p>Note: If the result is an instance of Throwable, this future will be
     * completed exceptionally. That is, {@link #get} will throw the exception
     * rather than return it.
     */
    public SimpleCompletedFuture(E result) {
        this.result = result;
    }

    public SimpleCompletedFuture(Throwable exceptionalResult) {
        this.result = exceptionalResult;
    }

    @SuppressWarnings("unchecked")
    @Override
    public E join() {
        if (result instanceof Throwable) {
            sneakyThrow((Throwable) result);
        }
        return (E) result;
    }

    @Override
    public boolean complete(Object value) {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void andThen(ExecutionCallback<E> callback) {
        if (result instanceof Throwable) {
            callback.onFailure((Throwable) result);
        } else {
            callback.onResponse((E) result);
        }
    }

    @Override
    public void andThen(final ExecutionCallback<E> callback, Executor executor) {
        executor.execute(new Runnable() {
            @SuppressWarnings("unchecked")
            @Override
            public void run() {
                if (result instanceof Throwable) {
                    callback.onFailure((Throwable) result);
                } else {
                    callback.onResponse((E) result);
                }
            }
        });
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return result instanceof CancellationException;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public E get() throws ExecutionException {
        if (result instanceof Throwable) {
            throw new ExecutionException((Throwable) result);
        }
        return (E) result;
    }

    @Override
    public E get(long timeout, TimeUnit unit) throws ExecutionException {
        return get();
    }
}
