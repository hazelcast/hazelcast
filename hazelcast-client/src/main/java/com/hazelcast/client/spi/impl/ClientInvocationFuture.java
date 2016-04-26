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

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.AbstractInvocationFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.util.ExceptionUtil.fixAsyncStackTrace;

public class ClientInvocationFuture extends AbstractInvocationFuture<ClientMessage> {

    protected final ClientMessage request;
    private final ClientInvocation invocation;

    public ClientInvocationFuture(ClientInvocation invocation, HazelcastClientInstanceImpl client,
                                  ClientMessage request, ILogger logger) {

        super(client.getClientExecutionService(), logger);
        this.request = request;
        this.invocation = invocation;
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
    public ClientMessage resolveAndThrow(Object response) throws ExecutionException, InterruptedException {
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
        return (ClientMessage) response;
    }

    public ClientInvocation getInvocation() {
        return invocation;
    }
}

