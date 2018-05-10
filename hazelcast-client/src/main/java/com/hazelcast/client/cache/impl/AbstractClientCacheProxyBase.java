/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;

import javax.cache.CacheException;
import javax.cache.integration.CompletionListener;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    private static final int TIMEOUT = 10;

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

    protected ILogger logger;

    boolean statisticsEnabled;
    CacheStatsHandler statsHandler;

    private final ConcurrentMap<Future, CompletionListener> loadAllCalls = new ConcurrentHashMap<Future, CompletionListener>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicBoolean isDestroyed = new AtomicBoolean(false);

    private final AtomicInteger completionIdCounter = new AtomicInteger();

    protected AbstractClientCacheProxyBase(CacheConfig<K, V> cacheConfig, ClientContext context) {
        super(ICacheService.SERVICE_NAME, cacheConfig.getName(), context);
        this.name = cacheConfig.getName();
        this.nameWithPrefix = cacheConfig.getNameWithPrefix();
        this.cacheConfig = cacheConfig;
        this.statisticsEnabled = cacheConfig.isStatisticsEnabled();
    }

    protected void injectDependencies(Object obj) {
        ManagedContext managedContext = getSerializationService().getManagedContext();
        managedContext.initialize(obj);
    }

    @Override
    protected void onInitialize() {
        logger = getContext().getLoggingService().getLogger(getClass());
        statsHandler = new CacheStatsHandler(getSerializationService());
    }

    @Override
    protected String getDistributedObjectName() {
        return cacheConfig.getNameWithPrefix();
    }

    protected int nextCompletionId() {
        return completionIdCounter.incrementAndGet();
    }

    void ensureOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Cache operations can not be performed. The cache closed");
        }
    }

    @Override
    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }
        close0(false);
    }

    private void close0(boolean destroy) {
        waitOnGoingLoadAllCallsToFinish();
        closeListeners();
        if (!destroy) {
            // when cache is being destroyed, the CacheManager is still required for cleanup and reset in postDestroy
            // when cache is being closed, the CacheManager is reset now
            resetCacheManager();
        }
    }

    private void waitOnGoingLoadAllCallsToFinish() {
        Iterator<Map.Entry<Future, CompletionListener>> iterator = loadAllCalls.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Future, CompletionListener> entry = iterator.next();
            Future future = entry.getKey();
            CompletionListener completionListener = entry.getValue();
            try {
                future.get(TIMEOUT, TimeUnit.SECONDS);
            } catch (Throwable t) {
                logger.finest("Error occurred at loadAll operation execution while waiting it to finish on cache close!", t);
                handleFailureOnCompletionListener(completionListener, t);
            }
            iterator.remove();
        }
    }

    @Override
    protected boolean preDestroy() {
        if (!isDestroyed.compareAndSet(false, true)) {
            return false;
        }
        close0(true);
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
    protected <T> T invoke(ClientMessage clientMessage) {
        try {
            Future<ClientMessage> future = new ClientInvocation(getClient(), clientMessage, getName()).invoke();
            return (T) future.get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected ClientMessage invoke(ClientMessage clientMessage, Data keyData) {
        try {
            int partitionId = getContext().getPartitionService().getPartitionId(keyData);
            Future future = new ClientInvocation(getClient(), clientMessage, getName(), partitionId).invoke();
            return (ClientMessage) future.get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected void submitLoadAllTask(ClientMessage request, CompletionListener completionListener, final List<Data> binaryKeys) {
        final CompletionListener listener = completionListener != null ? completionListener : NULL_COMPLETION_LISTENER;
        ClientDelegatingFuture<V> delegatingFuture = null;
        try {
            injectDependencies(completionListener);

            final long startNanos = nowInNanosOrDefault();
            ClientInvocationFuture future = new ClientInvocation(getClient(), request, getName()).invoke();
            delegatingFuture = newDelegatingFuture(future, LOAD_ALL_DECODER);
            final Future delFuture = delegatingFuture;
            loadAllCalls.put(delegatingFuture, listener);
            delegatingFuture.andThen(new ExecutionCallback<V>() {
                @Override
                public void onResponse(V response) {
                    loadAllCalls.remove(delFuture);
                    onLoadAll(binaryKeys, response, startNanos);
                    listener.onCompletion();
                }

                @Override
                public void onFailure(Throwable t) {
                    loadAllCalls.remove(delFuture);
                    handleFailureOnCompletionListener(listener, t);
                }
            });
        } catch (Throwable t) {
            if (delegatingFuture != null) {
                loadAllCalls.remove(delegatingFuture);
            }
            handleFailureOnCompletionListener(listener, t);
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

    protected void onLoadAll(List<Data> keys, Object response, long startNanos) {
    }

    long nowInNanosOrDefault() {
        return statisticsEnabled ? System.nanoTime() : -1;
    }

    protected <T> ClientDelegatingFuture<T> newDelegatingFuture(ClientInvocationFuture future, ClientMessageDecoder decoder) {
        return new ClientDelegatingFuture<T>(future, getSerializationService(), decoder);
    }

    protected <T> ClientDelegatingFuture<T> newDelegatingFuture(ClientInvocationFuture future, ClientMessageDecoder decoder,
                                                                boolean deserializeResponse) {
        return new ClientDelegatingFuture<T>(future, getSerializationService(), decoder, deserializeResponse);
    }
}
