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
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ExceptionUtil;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

/**
 * A specialization of {@link ClientDelegatingFuture} that allows to set an interceptor to be executed
 * from user thread on {@link #get()}, {@link #join()} or {@link #joinInternal()}.
 * @param <V>
 */
public class ClientInterceptingDelegatingFuture<V> extends ClientDelegatingFuture<V> {

    private final BiConsumer<V, Throwable> interceptor;

    public ClientInterceptingDelegatingFuture(ClientInvocationFuture clientInvocationFuture,
                                              SerializationService serializationService,
                                              ClientMessageDecoder clientMessageDecoder, BiConsumer<V, Throwable> interceptor) {
        super(clientInvocationFuture, serializationService, clientMessageDecoder);
        this.interceptor = interceptor;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        Throwable originalThrowable = null;
        Throwable throwable = null;
        ClientMessage response = null;
        try {
            response = (ClientMessage) future.get();
        } catch (Throwable t) {
            originalThrowable = t;
            throwable = (t instanceof ExecutionException) ? t.getCause() : t;
        }
        return intercept(originalThrowable, throwable, response);
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {
        Throwable originalThrowable = null;
        Throwable throwable = null;
        ClientMessage response = null;
        try {
            response = (ClientMessage) future.get(timeout, unit);
        } catch (Throwable t) {
            originalThrowable = t;
            throwable = (t instanceof ExecutionException) ? t.getCause() : t;
        }
        return intercept(originalThrowable, throwable, response);
    }

    @Override
    public V join() {
        Throwable originalThrowable = null;
        Throwable throwable = null;
        ClientMessage response = null;
        try {
            response = (ClientMessage) future.join();
        } catch (Throwable t) {
            originalThrowable = t;
            throwable = (t instanceof CompletionException) ? t.getCause() : t;
        }
        return intercept(originalThrowable, throwable, response);
    }

    @Override
    public V joinInternal() {
        Throwable throwable = null;
        ClientMessage response = null;
        try {
            response = ((ClientInvocationFuture) future).joinInternal();
        } catch (Throwable t) {
            throwable = t;
        }
        return intercept(throwable, throwable, response);
    }

    private V intercept(Throwable originalThrowable, Throwable throwable, ClientMessage response) {
        if (throwable != null) {
            interceptor.accept(null, throwable);
            throw ExceptionUtil.sneakyThrow(originalThrowable);
        } else {
            final V value = resolve(response);
            interceptor.accept(value, null);
            return value;
        }
    }
}
