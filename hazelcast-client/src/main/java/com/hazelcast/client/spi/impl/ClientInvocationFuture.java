/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.AbstractInvocationFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.util.ExceptionUtil.fixAsyncStackTrace;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

public class ClientInvocationFuture<E> extends AbstractInvocationFuture<E> {

    private static final AtomicReferenceFieldUpdater<ClientInvocationFuture, Object> DESERIALIZED_VALUE
            = newUpdater(ClientInvocationFuture.class, Object.class, "deserializedValue");

    protected final ClientMessage request;
    private final ClientInvocation invocation;
    private final ClientMessageDecoder decoder;
    private final HazelcastClientInstanceImpl client;
    // client caches the deserialized value
    private volatile Object deserializedValue;

    public ClientInvocationFuture(ClientInvocation invocation,
                                  HazelcastClientInstanceImpl client,
                                  ClientMessage request,
                                  ILogger logger,
                                  ClientMessageDecoder decoder) {
        super(client.getClientExecutionService(), logger);
        this.client = client;
        this.request = request;
        this.invocation = invocation;
        this.decoder = decoder;
    }

    @Override
    protected String invocationToString() {
        return request.toString();
    }

    @Override
    protected void onInterruptDetected() {
        complete(new InterruptedException());
    }

    @Override
    protected TimeoutException newTimeoutException(long timeout, TimeUnit unit) {
        return new TimeoutException();
    }

    @Override
    protected Throwable unwrap(Throwable throwable) {
        return throwable;
    }

    @Override
    protected Object resolve(Object value) {
        if (deserializedValue == null && decoder != null) {
            if (value instanceof ClientMessage) {
                value = decoder.decodeClientMessage((ClientMessage) value);
            }

            value = client.getSerializationService().toObject(value);

            // the client future apparently wants to have a shared deserialized value
            // It could be that multiple threads deserialize the same blob concurrently, but the alternative is locking.
            // and using this approach we keep it lock free.
            for (; ; ) {
                Object currentValue = deserializedValue;

                if (currentValue != null) {
                    // something already has been deserialized; so lets use that.
                    value = currentValue;
                    break;
                }

                if (DESERIALIZED_VALUE.compareAndSet(this, null, value)) {
                    // we successfully managed to store the deserialized value, so we can use that
                    break;
                }
            }
        }

        if (value instanceof Throwable) {
            return new ExecutionException((Throwable) value);
        }

        return value;
    }

    @Override
    public E resolveAndThrow(Object response) throws ExecutionException, InterruptedException {
        response = resolve(response);

        if (response instanceof Throwable) {
            fixAsyncStackTrace((Throwable) response, Thread.currentThread().getStackTrace());
            if (response instanceof ExecutionException) {
                throw (ExecutionException) response;
            }
            if (response instanceof Error) {
                throw (Error) response;
            }
            if (response instanceof InterruptedException) {
                throw (InterruptedException) response;
            }
            throw new ExecutionException((Throwable) response);
        }

        return (E) response;
    }

    public ClientInvocation getInvocation() {
        return invocation;
    }
}

