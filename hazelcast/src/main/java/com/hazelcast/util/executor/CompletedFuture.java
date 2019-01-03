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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.util.ExceptionUtil.rethrow;

public final class CompletedFuture<V> implements InternalCompletableFuture<V> {

    private final SerializationService serializationService;
    private final Executor userExecutor;
    private final Object value;

    public CompletedFuture(SerializationService serializationService, Object value, Executor userExecutor) {
        this.serializationService = serializationService;
        this.userExecutor = userExecutor;
        this.value = value;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        Object object = value;
        if (object instanceof Data) {
            object = serializationService.toObject(object);
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
    public V join() {
        try {
            // this method is quite inefficient when there is unchecked exception, because it will be wrapped
            // in a ExecutionException, and then it is unwrapped again.
            return get();
        } catch (Throwable throwable) {
            throw rethrow(throwable);
        }
    }

    @Override
    public boolean complete(Object value) {
        return false;
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
        andThen(callback, userExecutor);
    }

    @Override
    public void andThen(final ExecutionCallback<V> callback, final Executor executor) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                Object object = value;
                if (object instanceof Data) {
                    object = serializationService.toObject(object);
                }

                if (object instanceof Throwable) {
                    callback.onFailure((Throwable) object);
                } else {
                    callback.onResponse((V) object);
                }
            }
        });
    }
}
