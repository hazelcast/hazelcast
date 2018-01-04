/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Client Delegating Future is used to delegate ClientInvocationFuture to user to be used with
 * andThen or get. It converts ClientMessage coming from ClientInvocationFuture to user object
 *
 * @param <V> Value type that user expecting
 */
public class ClientDelegatingFuture<V> implements InternalCompletableFuture<V> {

    private static final AtomicReferenceFieldUpdater<ClientDelegatingFuture, Object> DECODED_RESPONSE =
            AtomicReferenceFieldUpdater.newUpdater(ClientDelegatingFuture.class, Object.class, "decodedResponse");
    private static final Object VOID = "VOID";
    private final ClientInvocationFuture future;
    private final SerializationService serializationService;
    private final ClientMessageDecoder clientMessageDecoder;
    private final boolean deserializeResponse;
    private final V defaultValue;
    private final Executor userExecutor;
    private volatile Object decodedResponse = VOID;

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
    public <T> void andThenInternal(ExecutionCallback<T> callback, boolean shouldDeserializeData) {
        future.andThen(new DelegatingExecutionCallback<T>(callback, shouldDeserializeData));
    }

    @Override
    public void andThen(ExecutionCallback<V> callback) {
        future.andThen(new DelegatingExecutionCallback<V>(callback, true), userExecutor);
    }

    @Override
    public void andThen(ExecutionCallback<V> callback, Executor executor) {
        future.andThen(new DelegatingExecutionCallback<V>(callback, true), executor);
    }

    @Override
    public boolean complete(Object value) {
        return future.complete(value);
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
        ClientMessage response = future.get(timeout, unit);
        return (V) resolveResponse(response, deserializeResponse);
    }

    @Override
    public V join() {
        try {
            return get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private Object resolveResponse(ClientMessage clientMessage, boolean deserialize) {
        if (defaultValue != null) {
            return defaultValue;
        }

        Object decodedResponse = decodeResponse(clientMessage);
        if (deserialize) {
            return serializationService.toObject(decodedResponse);
        }
        return decodedResponse;
    }

    private Object decodeResponse(ClientMessage clientMessage) {
        if (decodedResponse != VOID) {
            return decodedResponse;
        }
        ClientMessage message = ClientMessage.createForDecode(clientMessage.buffer(), 0);
        Object newDecodedResponse = clientMessageDecoder.decodeClientMessage(message);

        DECODED_RESPONSE.compareAndSet(this, VOID, newDecodedResponse);
        return newDecodedResponse;
    }

    protected ClientInvocationFuture getFuture() {
        return future;
    }

    class DelegatingExecutionCallback<T> implements ExecutionCallback<ClientMessage> {

        private final ExecutionCallback<T> callback;
        private final boolean deserialize;

        DelegatingExecutionCallback(ExecutionCallback<T> callback, boolean deserialize) {
            this.callback = callback;
            this.deserialize = deserialize;
        }

        @Override
        public void onResponse(ClientMessage message) {
            Object response = resolveResponse(message, deserialize);
            callback.onResponse((T) response);
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
    }
}
