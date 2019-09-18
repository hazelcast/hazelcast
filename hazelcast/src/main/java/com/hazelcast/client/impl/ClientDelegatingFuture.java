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

package com.hazelcast.client.impl;

import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * The Client Delegating Future is used to delegate {@link
 * ClientInvocationFuture} to a user type to be used with {@code andThen()} or
 * {@code get()}. It converts {@link ClientMessage} coming from {@link
 * ClientInvocationFuture} to a user object.
 *
 * @param <V> Value type that the user expects
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

    @Override
    public void andThen(@Nonnull ExecutionCallback<V> callback) {
        future.andThen(new DelegatingExecutionCallback<V>(callback, deserializeResponse), userExecutor);
    }

    @Override
    public void andThen(@Nonnull ExecutionCallback<V> callback, Executor executor) {
        future.andThen(new DelegatingExecutionCallback<V>(callback, deserializeResponse), executor);
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
            throw ExceptionUtil.sneakyThrow(e);
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
        //Since frames are read, there should be no need to re-read to client message
        Object newDecodedResponse = clientMessageDecoder.decodeClientMessage(clientMessage);

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
