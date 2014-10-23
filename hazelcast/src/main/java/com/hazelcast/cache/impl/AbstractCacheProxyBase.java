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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheOperationProvider;
import com.hazelcast.cache.impl.operation.CacheDestroyOperation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.executor.CompletableFutureTask;

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
 * support methods for cache proxy
 */
abstract class AbstractCacheProxyBase<K, V> {

    static final int TIMEOUT = 10;
    protected final CacheConfig<K, V> cacheConfig;
    //this will represent the name from the user perspective
    protected final String name;
    protected final String nameWithPrefix;
    protected final ICacheService cacheService;
    protected final SerializationService serializationService;
    protected final CacheOperationProvider operationProvider;

    private final NodeEngineImpl nodeEngine;
    private final CopyOnWriteArrayList<Future> loadAllTasks = new CopyOnWriteArrayList<Future>();
    private final CacheLoader<K, V> cacheLoader;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicBoolean isDestroyed = new AtomicBoolean(false);

    protected AbstractCacheProxyBase(CacheConfig cacheConfig, NodeEngineImpl nodeEngine, ICacheService cacheService) {
        this.name = cacheConfig.getName();
        this.nameWithPrefix = cacheConfig.getNameWithPrefix();
        this.cacheConfig = cacheConfig;
        this.nodeEngine = nodeEngine;
        this.cacheService = cacheService;
        this.serializationService = this.nodeEngine.getSerializationService();
        if (cacheConfig.getCacheLoaderFactory() != null) {
            final Factory<CacheLoader> cacheLoaderFactory = cacheConfig.getCacheLoaderFactory();
            cacheLoader = cacheLoaderFactory.create();
        } else {
            cacheLoader = null;
        }

        operationProvider = cacheService.getCacheOperationProvider(nameWithPrefix, cacheConfig.getInMemoryFormat());
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
        Operation operation = new CacheDestroyOperation(name);
        int partitionId = getNodeEngine().getPartitionService().getPartitionId(getDistributedObjectName());
        OperationService operationService = getNodeEngine().getOperationService();
        InternalCompletableFuture f = operationService.invokeOnPartition(CacheService.SERVICE_NAME, operation, partitionId);
        f.getSafely();
        //TODO @ali this causes pooled off-heap fail
        cacheService.destroyCache(getDistributedObjectName(), true, null);
    }

    public boolean isClosed() {
        return isClosed.get();
    }

    protected abstract void closeListeners();
    //endregion close&destroy

    //region DISTRIBUTED OBJECT
    protected String getDistributedObjectName() {
        return nameWithPrefix;
    }

    protected String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    protected ICacheService getService() {
        return cacheService;
    }

    protected NodeEngine getNodeEngine() {
        if (nodeEngine == null || !nodeEngine.isActive()) {
            throw new HazelcastInstanceNotActiveException();
        }
        return nodeEngine;
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

    protected void submitLoadAllTask(final OperationFactory operationFactory, final CompletionListener completionListener) {
        final LoadAllTask loadAllTask = new LoadAllTask(operationFactory, completionListener);
        final ExecutionService executionService = nodeEngine.getExecutionService();
        final CompletableFutureTask<?> future = (CompletableFutureTask<?>) executionService
                .submit("loadAll-" + nameWithPrefix, loadAllTask);
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

        private final OperationFactory operationFactory;
        private final CompletionListener completionListener;

        private LoadAllTask(OperationFactory operationFactory, CompletionListener completionListener) {
            this.operationFactory = operationFactory;
            this.completionListener = completionListener;
        }

        @Override
        public void run() {
            try {
                final Map<Integer, Object> results = getNodeEngine().getOperationService()
                        .invokeOnAllPartitions(getServiceName(), operationFactory);
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
