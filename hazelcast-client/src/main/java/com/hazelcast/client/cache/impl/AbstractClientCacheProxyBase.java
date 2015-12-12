/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.ICacheInternal;
import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheDestroyCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;

import javax.cache.CacheException;
import javax.cache.integration.CompletionListener;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract class providing cache open/close operations and {@link ClientContext} accessor which will be used
 * by implementation of {@link com.hazelcast.cache.ICache} for client.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
abstract class AbstractClientCacheProxyBase<K, V> implements ICacheInternal<K, V> {

    private static final ClientMessageDecoder LOAD_ALL_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) Boolean.TRUE;
        }
    };

    protected final ClientContext clientContext;
    protected final CacheConfig<K, V> cacheConfig;
    //this will represent the name from the user perspective
    protected final String name;
    protected final String nameWithPrefix;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicBoolean isDestroyed = new AtomicBoolean(false);

    private final AtomicInteger completionIdCounter = new AtomicInteger();

    protected AbstractClientCacheProxyBase(CacheConfig cacheConfig, ClientContext clientContext) {
        this.name = cacheConfig.getName();
        this.nameWithPrefix = cacheConfig.getNameWithPrefix();
        this.cacheConfig = cacheConfig;
        this.clientContext = clientContext;
    }

    protected int nextCompletionId() {
        return completionIdCounter.incrementAndGet();
    }

    //region close&destroy
    protected void ensureOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Cache operations can not be performed. The cache closed");
        }
    }

    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }
        closeListeners();
    }

    public void destroy() {
        close();
        if (!isDestroyed.compareAndSet(false, true)) {
            return;
        }
        isClosed.set(true);
        try {
            int partitionId = clientContext.getPartitionService().getPartitionId(nameWithPrefix);
            ClientMessage request = CacheDestroyCodec.encodeRequest(nameWithPrefix);
            final ClientInvocation clientInvocation = new ClientInvocation(
                    (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance(), request, partitionId);
            final Future<ClientMessage> future = clientInvocation.invoke();
            future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public boolean isClosed() {
        return isClosed.get();
    }

    public boolean isDestroyed() {
        return isDestroyed.get();
    }

    public void open() {
        if (isDestroyed.get()) {
            throw new IllegalStateException("Cache is already destroyed! Cannot be reopened");
        }
        if (!isClosed.compareAndSet(true, false)) {
            return;
        }
    }

    protected abstract void closeListeners();
    //endregion close&destroy

    //region DISTRIBUTED OBJECT
    public String getNameWithPrefix() {
        return nameWithPrefix;
    }

    protected <T> T toObject(Object data) {
        return clientContext.getSerializationService().toObject(data);
    }

    protected Data toData(Object o) {
        return clientContext.getSerializationService().toData(o);
    }

    protected ClientMessage invoke(ClientMessage clientMessage) {
        try {
            final Future<ClientMessage> future = new ClientInvocation(
                    (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance(), clientMessage).invoke();
            return future.get();

        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    protected ClientMessage invoke(ClientMessage clientMessage, Data keyData) {
        try {
            final int partitionId = clientContext.getPartitionService().getPartitionId(keyData);
            final Future future = new ClientInvocation((HazelcastClientInstanceImpl) clientContext.getHazelcastInstance(),
                    clientMessage, partitionId).invoke();
            return (ClientMessage) future.get();

        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    protected void submitLoadAllTask(final ClientMessage request, final CompletionListener completionListener,
                                     final Set<Data> keys) {
        try {
            final long start = System.nanoTime();
            ClientInvocationFuture future = new ClientInvocation(
                    (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance(), request).invoke();
            SerializationService serializationService = clientContext.getSerializationService();
            final ClientDelegatingFuture<V> delegatingFuture =
                    new ClientDelegatingFuture(future, serializationService, LOAD_ALL_DECODER);
            delegatingFuture.andThen(new ExecutionCallback<V>() {
                @Override
                public void onResponse(V response) {
                    if (completionListener != null) {
                        completionListener.onCompletion();
                        onLoadAll(keys, response, start, System.nanoTime());
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    if (completionListener != null) {
                        completionListener.onException(new CacheException(t));
                    }
                }
            });

        } catch (Exception e) {
            if (completionListener != null) {
                completionListener.onException(e);
            }
        } catch (Throwable t) {
            if (t instanceof OutOfMemoryError) {
                ExceptionUtil.rethrow(t);
            } else {
                if (completionListener != null) {
                    completionListener.onException(new CacheException(t));
                }
            }
        }
    }

    protected void onLoadAll(Set<Data> keys, Object response, long start, long end) {

    }
    //endregion CacheLoader
}
