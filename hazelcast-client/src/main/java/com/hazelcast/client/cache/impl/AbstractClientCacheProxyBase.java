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

import com.hazelcast.cache.impl.ICacheInternal;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import javax.cache.CacheException;
import javax.cache.integration.CompletionListener;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Abstract class providing cache open/close operations and {@link ClientContext} accessor which will be used
 * by implementation of {@link com.hazelcast.cache.ICache} for client.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
abstract class AbstractClientCacheProxyBase<K, V> extends ClientProxy implements ICacheInternal<K, V> {

    static final int TIMEOUT = 10;

    @SuppressWarnings("unchecked")
    private static final ClientMessageDecoder LOAD_ALL_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) Boolean.TRUE;
        }
    };

    private static final CompletionListener NULL_COMPLETION_LISTENER = new CompletionListener() {
        @Override
        public void onCompletion() {
        }

        @Override
        public void onException(Exception e) {
        }
    };

    // this will represent the name from the user perspective
    protected final String name;
    protected final String nameWithPrefix;
    protected final CacheConfig<K, V> cacheConfig;

    protected ClientContext clientContext;
    protected ILogger logger;

    private final ConcurrentMap<Future, CompletionListener> loadAllCalls
            = new ConcurrentHashMap<Future, CompletionListener>();

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicBoolean isDestroyed = new AtomicBoolean(false);

    private final AtomicInteger completionIdCounter = new AtomicInteger();

    protected AbstractClientCacheProxyBase(CacheConfig<K, V> cacheConfig) {
        super(ICacheService.SERVICE_NAME, cacheConfig.getName());
        this.name = cacheConfig.getName();
        this.nameWithPrefix = cacheConfig.getNameWithPrefix();
        this.cacheConfig = cacheConfig;
    }

    protected void injectDependencies(Object obj) {
        if (obj instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) obj).setHazelcastInstance(clientContext.getHazelcastInstance());
        }
    }

    @Override
    protected void onInitialize() {
        clientContext = getContext();
        logger = clientContext.getLoggingService().getLogger(getClass());
    }

    @Override
    protected String getDistributedObjectName() {
        return cacheConfig.getNameWithPrefix();
    }

    protected int nextCompletionId() {
        return completionIdCounter.incrementAndGet();
    }

    protected void ensureOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Cache operations can not be performed. The cache closed");
        }
    }

    @Override
    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }
        waitOnGoingLoadAllCallsToFinish();
        closeListeners();
    }

    private void waitOnGoingLoadAllCallsToFinish() {
        Iterator<Map.Entry<Future, CompletionListener>> iterator = loadAllCalls.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Future, CompletionListener> entry = iterator.next();
            Future f = entry.getKey();
            CompletionListener completionListener = entry.getValue();
            try {
                f.get(TIMEOUT, TimeUnit.SECONDS);
            } catch (Throwable t) {
                logger.finest("Error occurred at loadAll operation execution while waiting it to finish on cache close!", t);
                handleFailureOnCompletionListener(completionListener, t);
            }
            iterator.remove();
        }
    }

    @Override
    protected boolean preDestroy() {
        close();
        if (!isDestroyed.compareAndSet(false, true)) {
            return false;
        }
        isClosed.set(true);
        return true;
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public boolean isDestroyed() {
        return isDestroyed.get();
    }

    @Override
    public void open() {
        if (isDestroyed.get()) {
            throw new IllegalStateException("Cache is already destroyed! Cannot be reopened");
        }
        isClosed.set(false);
    }

    protected abstract void closeListeners();

    @Override
    public String getPrefixedName() {
        return nameWithPrefix;
    }

    /**
     * Gets the full cache name with prefixes.
     *
     * @return the full cache name with prefixes
     * @deprecated use #getPrefixedName instead
     */
    @Deprecated
    public String getNameWithPrefix() {
        return getPrefixedName();
    }

    @Override
    protected <T> T toObject(Object data) {
        return clientContext.getSerializationService().toObject(data);
    }

    @Override
    protected Data toData(Object o) {
        return clientContext.getSerializationService().toData(o);
    }

    @Override
    protected ClientMessage invoke(ClientMessage clientMessage) {
        try {
            Future<ClientMessage> future = new ClientInvocation(
                    (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance(), clientMessage).invoke();
            return future.get();

        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected ClientMessage invoke(ClientMessage clientMessage, Data keyData) {
        try {
            int partitionId = clientContext.getPartitionService().getPartitionId(keyData);
            Future future = new ClientInvocation((HazelcastClientInstanceImpl) clientContext.getHazelcastInstance(),
                    clientMessage, partitionId).invoke();
            return (ClientMessage) future.get();

        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected void submitLoadAllTask(ClientMessage request, CompletionListener completionListener, final Set<Data> keys) {
        final CompletionListener compListener = completionListener != null ? completionListener : NULL_COMPLETION_LISTENER;
        ClientDelegatingFuture<V> delegatingFuture = null;
        try {
            injectDependencies(completionListener);

            final long start = System.nanoTime();
            ClientInvocationFuture future = new ClientInvocation(
                    (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance(), request).invoke();
            SerializationService serializationService = clientContext.getSerializationService();
            delegatingFuture = new ClientDelegatingFuture<V>(future, serializationService, LOAD_ALL_DECODER);
            final Future delFuture = delegatingFuture;
            loadAllCalls.put(delegatingFuture, compListener);
            delegatingFuture.andThen(new ExecutionCallback<V>() {
                @Override
                public void onResponse(V response) {
                    loadAllCalls.remove(delFuture);
                    onLoadAll(keys, response, start, System.nanoTime());
                    compListener.onCompletion();
                }

                @Override
                public void onFailure(Throwable t) {
                    loadAllCalls.remove(delFuture);
                    handleFailureOnCompletionListener(compListener, t);
                }
            });
        } catch (Throwable t) {
            if (delegatingFuture != null) {
                loadAllCalls.remove(delegatingFuture);
            }
            handleFailureOnCompletionListener(compListener, t);
        }
    }

    private void handleFailureOnCompletionListener(CompletionListener completionListener, Throwable t) {
        if (t instanceof Exception) {
            Throwable cause = t.getCause();
            if (t instanceof ExecutionException && cause instanceof CacheException) {
                completionListener.onException((CacheException) cause);
            } else {
                completionListener.onException((Exception) t);
            }
        } else {
            if (t instanceof OutOfMemoryError) {
                throw rethrow(t);
            } else {
                completionListener.onException(new CacheException(t));
            }
        }
    }

    protected void onLoadAll(Set<Data> keys, Object response, long start, long end) {
    }
}
