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

package com.hazelcast.client.cache.impl;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A {@link ClientDelegatingFuture} which executes near-cache and cache statistics updates as synchronous operations
 * on get.
 */
abstract class CacheMaintenanceClientDelegatingFuture<V> extends ClientDelegatingFuture<V> {

    private static final AtomicIntegerFieldUpdater<CacheMaintenanceClientDelegatingFuture> ATOMIC_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(CacheMaintenanceClientDelegatingFuture.class, "executed");
    private static final int NOT_EXECUTED = 0;
    private static final int EXECUTED = 1;

    // responseData & deserializedValue are set once the future is resolved, before invoking
    // near-cache & statistics handling
    protected Object responseData;
    protected V deserializedValue;

    private final boolean nearCacheEnabled;
    private final boolean statsEnabled;
    private volatile int executed = NOT_EXECUTED;

    CacheMaintenanceClientDelegatingFuture(ClientInvocationFuture clientInvocationFuture,
                                           SerializationService serializationService,
                                           ClientMessageDecoder clientMessageDecoder,
                                           boolean nearCacheEnabled, boolean statsEnabled) {
        super(clientInvocationFuture, serializationService, clientMessageDecoder);
        this.nearCacheEnabled = nearCacheEnabled;
        this.statsEnabled = statsEnabled;
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
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            deserializedValue = super.get(timeout, unit);
            responseData = getResponse();
            // only execute nearcache/statistics update once
            if (ATOMIC_UPDATER.compareAndSet(this, NOT_EXECUTED, EXECUTED)) {
                if (nearCacheEnabled) {
                    handleNearCacheUpdate();
                }
                if (statsEnabled) {
                    handleStatistics();
                }
            }
            return deserializedValue;
        } catch (Throwable t) {
            return ExceptionUtil.sneakyThrow(t);
        }
    }

    public abstract void handleNearCacheUpdate();

    public abstract void handleStatistics();
}
