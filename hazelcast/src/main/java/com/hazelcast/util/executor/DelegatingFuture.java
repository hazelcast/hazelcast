/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DelegatingFuture<V> implements ICompletableFuture<V> {

    private final ICompletableFuture future;
    private final InternalSerializationService serializationService;
    private final V defaultValue;
    private final boolean hasDefaultValue;
    private final Object mutex = new Object();
    private Throwable error;
    private volatile V value;
    private volatile boolean done;

    public DelegatingFuture(ICompletableFuture future, SerializationService serializationService) {
        this(future, serializationService, null);
    }

    public DelegatingFuture(ICompletableFuture future, SerializationService serializationService, V defaultValue) {
        this.future = future;
        this.serializationService = (InternalSerializationService) serializationService;
        this.defaultValue = defaultValue;
        this.hasDefaultValue = defaultValue != null;
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


    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    @SuppressFBWarnings("IS2_INCONSISTENT_SYNC")
    public final V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (!done || value == null) {
            synchronized (mutex) {
                if (!done || value == null) {
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
            if (error instanceof CancellationException) {
                throw (CancellationException) error;
            }
            if (error.getCause() instanceof CancellationException) {
                throw (CancellationException) error.getCause();
            }
            if (error instanceof ExecutionException) {
                throw (ExecutionException) error;
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
            //todo do we need to call dispose data here
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

    protected ICompletableFuture getFuture() {
        return future;
    }

    @Override
    public void andThen(final ExecutionCallback<V> callback) {
        future.andThen(new DelegatingExecutionCallback<V>(callback));
    }

    @Override
    public void andThen(final ExecutionCallback<V> callback, Executor executor) {
        future.andThen(new DelegatingExecutionCallback<V>(callback), executor);
    }

    private class DelegatingExecutionCallback<T> implements ExecutionCallback {

        private final ExecutionCallback<T> callback;

        DelegatingExecutionCallback(ExecutionCallback<T> callback) {
            this.callback = callback;
        }

        @Override
        public void onResponse(Object response) {
            if (!done || value == null) {
                synchronized (mutex) {
                    if (!done || value == null) {
                        value = getResult(response);
                        done = true;
                    }
                }
            }
            callback.onResponse((T) value);
        }

        @Override
        public void onFailure(Throwable t) {
            if (!done) {
                synchronized (mutex) {
                    if (!done) {
                        error = t;
                        done = true;
                    }
                }
            }
            callback.onFailure(t);
        }
    }
}
