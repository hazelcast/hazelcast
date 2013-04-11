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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @mdogan 1/18/13
 */
public final class CompletedFuture<V> implements Future<V> {

    private final SerializationService serializationService;
    private final Object value;

    public CompletedFuture(SerializationService serializationService, Object value) {
        this.serializationService = serializationService;
        this.value = value;
    }

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

    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return get();
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    public boolean isCancelled() {
        return false;
    }

    public boolean isDone() {
        return true;
    }
}
