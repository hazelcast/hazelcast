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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class CompletedFuture<V> implements ICompletableFuture<V> {

    private final SerializationService serializationService;
    private final ExecutorService asyncExecutor;
    private final Object value;

    public CompletedFuture(SerializationService serializationService, Object value, ExecutorService asyncExecutor) {
        this.serializationService = serializationService;
        this.asyncExecutor = asyncExecutor;
        this.value = value;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        Object object = value;
        if (object instanceof Data) {
            object = serializationService.toObject((Data) object);
        }
        if (object instanceof Throwable) {
            if (object instanceof ExecutionException) {
                throw (ExecutionException) object;
            }
            if (object instanceof InterruptedException) {
                throw (InterruptedException) object;
            }
            throw new ExecutionException((Throwable) object);
        }
        return (V) object;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return get();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public void andThen(ExecutionCallback<V> callback) {
        andThen(callback, asyncExecutor);
    }

    @Override
    public void andThen(final ExecutionCallback<V> callback, final Executor executor) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                if (value instanceof Throwable) {
                    callback.onFailure((Throwable) value);
                } else {
                    callback.onResponse((V) value);
                }
            }
        });
    }
}
