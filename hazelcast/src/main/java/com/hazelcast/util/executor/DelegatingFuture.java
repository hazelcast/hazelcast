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
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.internal.serialization.SerializationService;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A {@link InternalCompletableFuture} implementation that delegates the real logic to an underlying
 * {@link InternalCompletableFuture} and decorates it with additional behavior:
 * <ol>
 * <li>change the returned value by setting the result</li>
 * <li>always deserializing the content</li>
 * <li>caching the deserialized content so that a deserialization only happens once. This should be used with
 * care since this could lead to unexpected sharing of object instances if the same future is shared between
 * threads.
 * </li>
 * </ol>
 * @param <V>
 */
public class DelegatingFuture<V> implements InternalCompletableFuture<V> {

    private static final AtomicReferenceFieldUpdater<DelegatingFuture, Object> DESERIALIZED_VALUE
            = AtomicReferenceFieldUpdater.newUpdater(DelegatingFuture.class, Object.class, "deserializedValue");

    private static final Object VOID = new Object() {
        @Override
        public String toString() {
            return "void";
        }
    };

    private final InternalCompletableFuture future;
    private final InternalSerializationService serializationService;
    private final Object result;
    private volatile Object deserializedValue = VOID;

    public DelegatingFuture(InternalCompletableFuture future, SerializationService serializationService) {
        this(future, serializationService, null);
    }

    /**
     * Creates a DelegatingFuture
     *
     * @param future               the underlying future to delegate to.
     * @param serializationService the {@link SerializationService}
     * @param result               the resuling value to be used when the underlying future completes. So no matter the return
     *                             value of that future, the result will be returned by the DelegatingFuture. A null
     *                             value means that the value of the underlying future should be used.
     */
    public DelegatingFuture(InternalCompletableFuture future, SerializationService serializationService, V result) {
        this.future = future;
        this.serializationService = (InternalSerializationService) serializationService;
        this.result = result;
    }

    @Override
    public final V get() throws InterruptedException, ExecutionException {
        return resolve(future.get());
    }

    @Override
    public final V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return resolve(future.get(timeout, unit));
    }

    private V resolve(Object object) {
        // if there is an explicit value set, we use that
        if (result != null) {
            return (V) result;
        }

        // if there already is a deserialized value set, use it.
        if (deserializedValue != VOID) {
            return (V) deserializedValue;
        }

        if (object instanceof Data) {
            // we need to deserialize.
            Data data = (Data) object;
            object = serializationService.toObject(data);

            //todo do we need to call dispose data here
            serializationService.disposeData(data);

            // now we need to try to set the value for other users.
            for (; ; ) {
                Object current = deserializedValue;
                if (current != VOID) {
                    object = current;
                    break;
                } else if (DESERIALIZED_VALUE.compareAndSet(this, VOID, object)) {
                    break;
                }
            }
        }
        return (V) object;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public final boolean isDone() {
        return future.isDone();
    }

    @Override
    public boolean complete(Object value) {
        return future.complete(value);
    }

    protected void setError(Throwable error) {
        future.complete(error);
    }

    protected ICompletableFuture getFuture() {
        return future;
    }

    @Override
    public V join() {
        return resolve(future.join());
    }

    @Override
    public void andThen(final ExecutionCallback<V> callback) {
        future.andThen(new DelegatingExecutionCallback(callback));
    }

    @Override
    public void andThen(final ExecutionCallback<V> callback, Executor executor) {
        future.andThen(new DelegatingExecutionCallback(callback), executor);
    }

    private class DelegatingExecutionCallback implements ExecutionCallback<V> {

        private final ExecutionCallback<V> callback;

        DelegatingExecutionCallback(ExecutionCallback<V> callback) {
            this.callback = callback;
        }

        @Override
        public void onResponse(Object response) {
            callback.onResponse(resolve(response));
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
    }
}
