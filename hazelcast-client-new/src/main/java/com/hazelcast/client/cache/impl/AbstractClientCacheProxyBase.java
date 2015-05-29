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
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheDestroyCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientExecutionServiceImpl;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;

import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CompletionListener;
import java.io.Closeable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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

    static final int TIMEOUT = 10;

    protected final ClientContext clientContext;
    protected final CacheConfig<K, V> cacheConfig;
    //this will represent the name from the user perspective
    protected final String name;
    protected final String nameWithPrefix;

    private final CopyOnWriteArrayList<Future> loadAllTasks = new CopyOnWriteArrayList<Future>();
    private CacheLoader<K, V> cacheLoader;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicBoolean isDestroyed = new AtomicBoolean(false);

    private final AtomicInteger completionIdCounter = new AtomicInteger();

    protected AbstractClientCacheProxyBase(CacheConfig cacheConfig, ClientContext clientContext) {
        this.name = cacheConfig.getName();
        this.nameWithPrefix = cacheConfig.getNameWithPrefix();
        this.cacheConfig = cacheConfig;
        this.clientContext = clientContext;
        init();
    }

    private void init() {
        if (cacheConfig.getCacheLoaderFactory() != null) {
            final Factory<CacheLoader<K, V>> cacheLoaderFactory = cacheConfig.getCacheLoaderFactory();
            cacheLoader = cacheLoaderFactory.create();
        } else {
            cacheLoader = null;
        }
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
        for (Future f : loadAllTasks) {
            try {
                f.get(TIMEOUT, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new CacheException(e);
            }
        }
        loadAllTasks.clear();
        //close the configured CacheLoader
        closeCacheLoader();
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
        init();
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

    //endregion DISTRIBUTED OBJECT

    //region CacheLoader
    protected void validateCacheLoader(CompletionListener completionListener) {
        if (cacheLoader == null && completionListener != null) {
            completionListener.onCompletion();
        }
    }

    protected void closeCacheLoader() {
        //close the configured CacheLoader
        if (cacheLoader instanceof Closeable) {
            IOUtil.closeResource((Closeable) cacheLoader);
        }
    }

    protected void submitLoadAllTask(final ClientMessage request, final CompletionListener completionListener) {
        LoadAllTask loadAllTask = new LoadAllTask(request, completionListener);
        ClientExecutionServiceImpl executionService = (ClientExecutionServiceImpl) clientContext.getExecutionService();

        final ICompletableFuture<?> future = executionService.submitInternal(loadAllTask);
        loadAllTasks.add(future);
        future.andThen(new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                loadAllTasks.remove(future);
            }

            @Override
            public void onFailure(Throwable t) {
                loadAllTasks.remove(future);
            }
        });
    }

    private final class LoadAllTask
            implements Runnable {

        private final ClientMessage request;
        private final CompletionListener completionListener;

        private LoadAllTask(ClientMessage request, CompletionListener completionListener) {
            this.request = request;
            this.completionListener = completionListener;
        }

        @Override
        public void run() {
            try {
                invoke(request);
                if (completionListener != null) {
                    completionListener.onCompletion();
                }

            } catch (Exception e) {
                if (completionListener != null) {
                    completionListener.onException(e);
                }
            }
        }
    }
    //endregion CacheLoader
}
