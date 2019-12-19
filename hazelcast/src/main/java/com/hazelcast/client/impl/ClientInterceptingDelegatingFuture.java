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
import com.hazelcast.internal.util.Clock;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;

/**
 * A specialization of {@link ClientDelegatingFuture} that allows to set an interceptor to be executed
 * from user thread on {@link #get()}, {@link #join()} or {@link #joinInternal()}.
 * The interceptor is executed once. Concurrent executions of {@code get()} or other blocking methods
 * will block until callback execution completes.
 * @param <V>
 */
public class ClientInterceptingDelegatingFuture<V> extends ClientDelegatingFuture<V> {

    private static final AtomicIntegerFieldUpdater CALL_STATE_UPDATER
            = AtomicIntegerFieldUpdater.newUpdater(ClientInterceptingDelegatingFuture.class, "callState");

    private static final int NOT_CALLED = 0;
    private static final int CALL_IN_PROGRESS = 1;
    private static final int CALL_FINISHED = 2;

    private volatile int callState = NOT_CALLED;
    private volatile Object interceptedValue;
    private volatile boolean completedExceptionally;

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
        return intercept(originalThrowable, throwable, response, Long.MAX_VALUE);
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {
        long deadline = Clock.currentTimeMillis() + unit.toMillis(timeout);
        Throwable originalThrowable = null;
        Throwable throwable = null;
        ClientMessage response = null;
        try {
            response = (ClientMessage) future.get(timeout, unit);
        } catch (Throwable t) {
            originalThrowable = t;
            throwable = (t instanceof ExecutionException) ? t.getCause() : t;
        }
        return intercept(originalThrowable, throwable, response, deadline);
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
        return intercept(originalThrowable, throwable, response, Long.MAX_VALUE);
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
        return intercept(throwable, throwable, response, Long.MAX_VALUE);
    }

    private V intercept(Throwable originalThrowable, Throwable throwable, ClientMessage response, long deadline) {
        if (callState == CALL_FINISHED) {
            returnOrThrow();
        }
        if (CALL_STATE_UPDATER.compareAndSet(this, NOT_CALLED, CALL_IN_PROGRESS)) {
            if (throwable != null) {
                interceptor.accept(null, throwable);
                interceptedValue = originalThrowable;
                completedExceptionally = true;
                callState = CALL_FINISHED;
            } else {
                final V value = resolve(response);
                interceptor.accept(value, null);
                interceptedValue = value;
                callState = CALL_FINISHED;
            }
        }
        while (callState != CALL_FINISHED) {
            if (Clock.currentTimeMillis() > deadline) {
                sneakyThrow(new TimeoutException("Callback did not complete within timeout"));
            }
        }
        return returnOrThrow();
    }

    private V returnOrThrow() {
        if (completedExceptionally) {
            throw sneakyThrow((Throwable) interceptedValue);
        } else {
            return (V) interceptedValue;
        }
    }
}
