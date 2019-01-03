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

package com.hazelcast.client.cache.impl;

import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.util.ExceptionUtil.sneakyThrow;

/**
 * A specific {@link ClientDelegatingFuture} implementation which calls given {@link OneShotExecutionCallback} as sync on get.
 */
class CallbackAwareClientDelegatingFuture<V> extends ClientDelegatingFuture<V> {

    private final OneShotExecutionCallback<V> callback;

    CallbackAwareClientDelegatingFuture(ClientInvocationFuture clientInvocationFuture,
                                        SerializationService serializationService,
                                        ClientMessageDecoder clientMessageDecoder,
                                        OneShotExecutionCallback<V> callback) {
        super(clientInvocationFuture, serializationService, clientMessageDecoder);
        this.callback = callback;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        try {
            V result = super.get();
            /*
             * - If it has not been called yet, it will be called and it will be waited to finish.
             * - If it has been called but not finished yet, it will be waited to finish.
             * - If it has been called and finished already, will return immediately.
             */
            callback.onResponse(result);
            return result;
        } catch (Throwable t) {
            callback.onFailure(t);
            return sneakyThrow(t);
        }
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        long finishTime = (timeout == Long.MAX_VALUE)
                ? Long.MAX_VALUE
                : Clock.currentTimeMillis() + unit.toMillis(timeout);
        if (finishTime < 0) {
            finishTime = Long.MAX_VALUE;
        }
        try {
            V result = super.get(timeout, unit);
            /*
             * - If it has not been called yet, it will be called and it will be waited to finish.
             * - If it has been called but not finished yet, it will be waited to finish.
             * - If it has been called and finished already, will return immediately.
             */
            callback.onResponse(result, finishTime);
            return result;
        } catch (Throwable t) {
            callback.onFailure(t, finishTime);
            return sneakyThrow(t);
        }
    }
}
