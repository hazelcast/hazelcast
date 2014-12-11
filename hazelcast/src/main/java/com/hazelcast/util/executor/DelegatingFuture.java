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

package com.hazelcast.util.executor;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DelegatingFuture<V> implements ICompletableFuture<V> {

    protected final ICompletableFuture future;
    private final SerializationService serializationService;
    private final V defaultValue;
    private final boolean hasDefaultValue;
    private V value;
    private Throwable error;
    private volatile boolean done;

    public DelegatingFuture(ICompletableFuture future, SerializationService serializationService) {
        this.future = future;
        this.serializationService = serializationService;
        this.defaultValue = null;
        this.hasDefaultValue = false;
    }

    public DelegatingFuture(ICompletableFuture future, SerializationService serializationService, V defaultValue) {
        this.future = future;
        this.serializationService = serializationService;
        this.defaultValue = defaultValue;
        this.hasDefaultValue = true;
    }

    @Override
    public final V get() throws InterruptedException, ExecutionException {
        try {
            return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // should not happen!
            return ExceptionUtil.sneakyThrow(e);
        }
    }


    @edu.umd.cs.findbugs.annotations.SuppressWarnings("IS2_INCONSISTENT_SYNC")
    @Override
    public final V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (!done) {
            synchronized (this) {
                if (!done) {
                    try {
                        value = getResult(future.get(timeout, unit));
                    } catch (InterruptedException e) {
                        error = e;
                    } catch (ExecutionException e) {
                        error = e;
                    }
                    done = true;
                }
            }
        }
        if (error != null) {
            if (error instanceof ExecutionException) {
                throw (ExecutionException) error;
            }
            if (error instanceof CancellationException) {
                throw (CancellationException) error;
            }
            if (error instanceof InterruptedException) {
                throw (InterruptedException) error;
            }
            // should not happen!
            throw new ExecutionException(error);
        }
        return value;
    }

    private V getResult(Object object) {
        if (hasDefaultValue) {
            return defaultValue;
        }
        if (object instanceof Data) {
            Data data = (Data) object;
            object = serializationService.toObject(data);
            serializationService.disposeData(data);
        }
        return (V) object;
    }


    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        done = true;
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public final boolean isDone() {
        return done ? done : future.isDone();
    }

    protected void setError(Throwable error) {
        this.error = error;
    }

    protected void setDone() {
        this.done = true;
    }

    @Override
    public void andThen(ExecutionCallback<V> callback) {
        future.andThen(callback);
    }

    @Override
    public void andThen(ExecutionCallback<V> callback, Executor executor) {
        future.andThen(callback, executor);
    }
}
