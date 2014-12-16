/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.impl.client.CacheDestroyRequest;
import com.hazelcast.cache.impl.client.CacheLoadAllRequest;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientExecutionService;
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
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.cache.impl.CacheProxyUtil.validateResults;

/**
 * Abstract class providing cache open/close operations and {@link ClientContext} accessor which will be used
 * by implementation of {@link com.hazelcast.cache.ICache} for client.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
abstract class AbstractClientCacheProxyBase<K, V> {

    static final int TIMEOUT = 10;
    protected final ClientContext clientContext;
    protected final CacheConfig<K, V> cacheConfig;
    //this will represent the name from the user perspective
    protected final String name;
    protected final String nameWithPrefix;

    private final CopyOnWriteArrayList<Future> loadAllTasks = new CopyOnWriteArrayList<Future>();
    private final CacheLoader<K, V> cacheLoader;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicBoolean isDestroyed = new AtomicBoolean(false);

    protected AbstractClientCacheProxyBase(CacheConfig cacheConfig, ClientContext clientContext) {
        this.name = cacheConfig.getName();
        this.nameWithPrefix = cacheConfig.getNameWithPrefix();
        this.cacheConfig = cacheConfig;
        this.clientContext = clientContext;
        if (cacheConfig.getCacheLoaderFactory() != null) {
            final Factory<CacheLoader> cacheLoaderFactory = cacheConfig.getCacheLoaderFactory();
            cacheLoader = cacheLoaderFactory.create();
        } else {
            cacheLoader = null;
        }
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
            CacheDestroyRequest request = new CacheDestroyRequest(nameWithPrefix, partitionId);
            final Future future = clientContext.getInvocationService().invokeOnKeyOwner(request, nameWithPrefix);
            future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public boolean isClosed() {
        return isClosed.get();
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

    protected <T> T invoke(ClientRequest req) {
        try {
            final Future future = clientContext.getInvocationService().invokeOnRandomTarget(req);
            Object result = future.get();
            return toObject(result);
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

    protected void submitLoadAllTask(final CacheLoadAllRequest request, final CompletionListener completionListener) {
        final LoadAllTask loadAllTask = new LoadAllTask(request, completionListener);
        final ClientExecutionService executionService = clientContext.getExecutionService();

        final ICompletableFuture<?> future = executionService.submit(loadAllTask);
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

        private final CacheLoadAllRequest request;
        private final CompletionListener completionListener;

        private LoadAllTask(CacheLoadAllRequest request, CompletionListener completionListener) {
            this.request = request;
            this.completionListener = completionListener;
        }

        @Override
        public void run() {
            try {
                final Map<Integer, Object> results = invoke(request);
                validateResults(results);
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
