/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.util;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ExecutorServiceSubmitToPartitionCodec;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
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

public class ClientDelegatingFuture<V> implements ICompletableFuture<V> {

    private final ClientInvocationFuture future;
    private final SerializationService serializationService;
    private final V defaultValue;
    private final Object mutex = new Object();
    private Throwable error;
    private V deserializedValue;
    private Data valueData;
    private volatile boolean done;

    public ClientDelegatingFuture(ClientInvocationFuture clientInvocationFuture,
                                  SerializationService serializationService, V defaultValue) {
        this.future = clientInvocationFuture;
        this.serializationService = serializationService;
        this.defaultValue = defaultValue;
    }

    public ClientDelegatingFuture(ClientInvocationFuture clientInvocationFuture,
                                  SerializationService serializationService) {
        this.future = clientInvocationFuture;
        this.serializationService = serializationService;
        this.defaultValue = null;
    }

    public void andThenInternal(final ExecutionCallback<Data> callback) {
        future.andThenInternal(new DelegatingExecutionCallback<Data>(callback, false));
    }

    @Override
    public void andThen(final ExecutionCallback<V> callback) {
        future.andThen(new DelegatingExecutionCallback<V>(callback, true));
    }

    @Override
    public void andThen(final ExecutionCallback<V> callback, Executor executor) {
        future.andThen(new DelegatingExecutionCallback<V>(callback, true), executor);
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

    @Override
    public V get() throws InterruptedException, ExecutionException {
        try {
            return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return ExceptionUtil.sneakyThrow(e);
        }
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {
        if (!done) {
            synchronized (mutex) {
                if (!done) {
                    try {
                        valueData = resolveMessageToValue(future.get(timeout, unit));
                        if (deserializedValue == null) {
                            deserializedValue = serializationService.toObject(valueData);
                        }
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
        return getResult();
    }

    private V getResult() {
        if (defaultValue != null) {
            return defaultValue;
        }

        // If value is already deserialized, use it.
        if (deserializedValue != null) {
            return deserializedValue;
        } else {
            // Otherwise, it is possible that received data may not be deserialized
            // if "shouldDeserializeData" flag is not true in any of registered "DelegatingExecutionCallback".
            // So, be sure that value is deserialized before returning to caller.
            if (valueData != null) {
                deserializedValue = serializationService.toObject(valueData);
            }
            return deserializedValue;
        }
    }

    private Data resolveMessageToValue(ClientMessage message) {
        return ExecutorServiceSubmitToPartitionCodec.decodeResponse(message).response;
    }

    protected void setError(Throwable error) {
        this.error = error;
    }

    protected void setDone() {
        this.done = true;
    }

    protected ClientInvocationFuture getFuture() {
        return future;
    }

    class DelegatingExecutionCallback<T> implements ExecutionCallback<ClientMessage> {

        private final ExecutionCallback<T> callback;
        private final boolean shouldDeserializeData;

        DelegatingExecutionCallback(ExecutionCallback<T> callback, boolean shouldDeserializeData) {
            this.callback = callback;
            this.shouldDeserializeData = shouldDeserializeData;
        }

        @Override
        public void onResponse(ClientMessage message) {
            if (!done) {
                synchronized (mutex) {
                    if (!done) {
                        valueData = resolveMessageToValue(message);
                        if (shouldDeserializeData && deserializedValue == null) {
                            deserializedValue = serializationService.toObject(valueData);
                        }
                        done = true;
                    }
                }
            }
            if (shouldDeserializeData) {
                callback.onResponse((T) deserializedValue);
            } else {
                callback.onResponse((T) valueData);
            }
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
