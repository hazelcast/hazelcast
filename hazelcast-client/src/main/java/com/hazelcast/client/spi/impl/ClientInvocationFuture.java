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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.AbstractInvocationFuture;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.hazelcast.util.ExceptionUtil.fixAsyncStackTrace;
import static com.hazelcast.util.ExceptionUtil.peel;
import static com.hazelcast.util.Preconditions.isNotNull;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

public class ClientInvocationFuture extends AbstractInvocationFuture<ClientMessage> {

    private final AtomicIntegerFieldUpdater COMPLETED_COUNT = newUpdater(ClientInvocationFuture.class, "completeCount");

    private final ClientMessage request;
    private final ClientInvocation invocation;
    private final CallIdSequence callIdSequence;
    private volatile int completeCount = 1;

    public ClientInvocationFuture(ClientInvocation invocation, Executor internalExecutor,
                                  ClientMessage request, ILogger logger,
                                  CallIdSequence callIdSequence) {
        super(internalExecutor, logger);
        this.request = request;
        this.invocation = invocation;
        this.callIdSequence = callIdSequence;
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
        if (value instanceof Throwable) {
            return new ExecutionException((Throwable) value);
        }
        return value;
    }

    @Override
    public void andThen(ExecutionCallback<ClientMessage> callback) {
        isNotNull(callback, "callback");

        if (completeCount == 0) {
            try {
                callback.onResponse(get());
            } catch (Exception e) {
                callback.onFailure(peel(e));
            }
            return;
        }
        super.andThen(new InternalDelegatingExecutionCallback(callback));
    }


    @Override
    protected void onComplete() {
        complete();
    }

    private void complete() {
        if (COMPLETED_COUNT.decrementAndGet(this) == 0) {
            callIdSequence.complete();
        }
    }

    @Override
    public ClientMessage resolveAndThrowIfException(Object response) throws ExecutionException, InterruptedException {
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
            if (response instanceof CancellationException) {
                throw (CancellationException) response;
            }
            throw new ExecutionException((Throwable) response);
        }
        return (ClientMessage) response;
    }

    public ClientInvocation getInvocation() {
        return invocation;
    }

    class InternalDelegatingExecutionCallback implements ExecutionCallback<ClientMessage> {

        private final ExecutionCallback<ClientMessage> callback;

        InternalDelegatingExecutionCallback(ExecutionCallback<ClientMessage> callback) {
            this.callback = callback;
            COMPLETED_COUNT.incrementAndGet(ClientInvocationFuture.this);
        }

        @Override
        public void onResponse(ClientMessage message) {
            try {
                callback.onResponse(message);
            } finally {
                complete();
            }
        }

        @Override
        public void onFailure(Throwable t) {
            try {
                callback.onFailure(t);
            } finally {
                complete();
            }
        }
    }
}

