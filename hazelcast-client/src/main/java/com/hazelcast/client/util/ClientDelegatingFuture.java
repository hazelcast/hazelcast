/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Client Delegating Future is used to delegate ClientInvocationFuture to user to be used with
 * andThen or get. It converts ClientMessage coming from ClientInvocationFuture to user object
 *
 * @param <V> Value type that user expecting
 */
public class ClientDelegatingFuture<V> implements InternalCompletableFuture<V> {

    private final ClientInvocationFuture future;
    private final SerializationService serializationService;
    private final ClientMessageDecoder clientMessageDecoder;
    private final boolean deserializeResponse;
    private final V defaultValue;
    private final Object mutex = new Object();
    private final Executor userExecutor;
    private Throwable error;
    private V deserializedValue;
    /**
     * mutex object is used as an initial NIL value
     */
    private volatile Object response = mutex;
    private volatile boolean done;

    public ClientDelegatingFuture(ClientInvocationFuture clientInvocationFuture,
                                  SerializationService serializationService,
                                  ClientMessageDecoder clientMessageDecoder, V defaultValue, boolean deserializeResponse) {
        this.future = clientInvocationFuture;
        this.serializationService = serializationService;
        this.clientMessageDecoder = clientMessageDecoder;
        this.defaultValue = defaultValue;
        this.userExecutor = clientInvocationFuture.getInvocation().getUserExecutor();
        this.deserializeResponse = deserializeResponse;
    }

    public ClientDelegatingFuture(ClientInvocationFuture clientInvocationFuture,
                                  SerializationService serializationService,
                                  ClientMessageDecoder clientMessageDecoder, V defaultValue) {
        this(clientInvocationFuture, serializationService, clientMessageDecoder, defaultValue, true);
    }

    public ClientDelegatingFuture(ClientInvocationFuture clientInvocationFuture,
                                  SerializationService serializationService,
                                  ClientMessageDecoder clientMessageDecoder) {
        this(clientInvocationFuture, serializationService, clientMessageDecoder, null, true);
    }

    public ClientDelegatingFuture(ClientInvocationFuture clientInvocationFuture,
                                  SerializationService serializationService,
                                  ClientMessageDecoder clientMessageDecoder, boolean deserializeResponse) {
        this(clientInvocationFuture, serializationService, clientMessageDecoder, null, deserializeResponse);
    }

    /**
     * Uses internal executor to execute callbacks instead of {@link #userExecutor}.
     * This method is intended to use by hazelcast internals.
     *
     * @param callback              callback to execute
     * @param shouldDeserializeData when {@code true} execution result is converted to object format
     *                              before passing to {@link ExecutionCallback#onResponse},
     *                              otherwise execution result will be in {@link com.hazelcast.nio.serialization.Data} format
     * @param <T>                   type of the execution result which is passed to {@link ExecutionCallback#onResponse}
     */
    public <T> void andThenInternal(final ExecutionCallback<T> callback, boolean shouldDeserializeData) {
        future.andThen(new DelegatingExecutionCallback<T>(callback, shouldDeserializeData));
    }

    @Override
    public void andThen(final ExecutionCallback<V> callback) {
        future.andThen(new DelegatingExecutionCallback<V>(callback, true), userExecutor);
    }

    @Override
    public void andThen(final ExecutionCallback<V> callback, Executor executor) {
        future.andThen(new DelegatingExecutionCallback<V>(callback, true), executor);
    }

    @Override
    public boolean complete(Object value) {
        return future.complete(value);
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

    public Object getResponse() {
        return response;
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
    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    public V get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {
        if (!done || !isResponseSet()) {
            synchronized (mutex) {
                if (!done || !isResponseSet()) {
                    try {
                        response = resolveMessageToValue(future.get(timeout, unit));
                        if (deserializeResponse && deserializedValue == null) {
                            deserializedValue = serializationService.toObject(response);
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
        return (V) getResult();
    }

    @Override
    public V join() {
        try {
            return get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private Object getResult() {
        if (defaultValue != null) {
            return defaultValue;
        }

        // If value is already deserialized, use it.
        if (deserializedValue != null) {
            return deserializedValue;
        }
        if (deserializeResponse) {
            // Otherwise, it is possible that received data may not be deserialized
            // if "shouldDeserializeData" flag is not true in any of registered "DelegatingExecutionCallback".
            // So, be sure that value is deserialized before returning to caller.
            deserializedValue = serializationService.toObject(response);
            return deserializedValue;
        }

        return response;
    }

    private Object resolveMessageToValue(ClientMessage message) {
        return clientMessageDecoder.decodeClientMessage(message);
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

    private boolean isResponseSet() {
        return response != mutex;
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
            if (!done || !isResponseSet()) {
                synchronized (mutex) {
                    if (!done || !isResponseSet()) {
                        response = resolveMessageToValue(message);
                        if (shouldDeserializeData && deserializedValue == null) {
                            deserializedValue = serializationService.toObject(response);
                        }
                        done = true;
                    }
                }
            }
            if (shouldDeserializeData) {
                callback.onResponse((T) deserializedValue);
            } else {
                callback.onResponse((T) response);
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
