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

package com.hazelcast.cache.impl;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.executor.CompletableFutureTask;

import javax.cache.CacheException;
import javax.cache.integration.CompletionListener;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.cache.impl.CacheProxyUtil.validateResults;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Abstract class providing cache open/close operations and {@link NodeEngine}, {@link CacheService} and
 * {@link SerializationService} accessor which will be used by implementation of {@link com.hazelcast.cache.ICache}
 * in server or embedded mode.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 * @see com.hazelcast.cache.impl.CacheProxy
 */
abstract class AbstractCacheProxyBase<K, V>
        extends AbstractDistributedObject<ICacheService>
        implements ICacheInternal<K, V> {

    private static final int TIMEOUT = 10;

    protected final ILogger logger;
    protected final CacheConfig<K, V> cacheConfig;
    protected final String name;
    protected final String nameWithPrefix;
    protected final ICacheService cacheService;
    protected final SerializationService serializationService;
    protected final CacheOperationProvider operationProvider;
    protected final IPartitionService partitionService;

    private final NodeEngine nodeEngine;
    private final CopyOnWriteArrayList<Future> loadAllTasks = new CopyOnWriteArrayList<Future>();

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicBoolean isDestroyed = new AtomicBoolean(false);

    AbstractCacheProxyBase(CacheConfig<K, V> cacheConfig, NodeEngine nodeEngine, ICacheService cacheService) {
        super(nodeEngine, cacheService);
        this.name = cacheConfig.getName();
        this.nameWithPrefix = cacheConfig.getNameWithPrefix();
        this.cacheConfig = cacheConfig;
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.partitionService = nodeEngine.getPartitionService();
        this.cacheService = cacheService;
        this.serializationService = nodeEngine.getSerializationService();
        this.operationProvider =
                cacheService.getCacheOperationProvider(nameWithPrefix, cacheConfig.getInMemoryFormat());
    }

    void injectDependencies(Object obj) {
        if (obj instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) obj).setHazelcastInstance(nodeEngine.getHazelcastInstance());
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    protected String getDistributedObjectName() {
        return nameWithPrefix;
    }

    @Override
    public String getPrefixedName() {
        return nameWithPrefix;
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public void open() {
        if (isDestroyed.get()) {
            throw new IllegalStateException("Cache is already destroyed! Cannot be reopened");
        }
        isClosed.compareAndSet(true, false);
    }

    @Override
    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }
        Exception caughtException = null;
        for (Future f : loadAllTasks) {
            try {
                f.get(TIMEOUT, TimeUnit.SECONDS);
            } catch (Exception e) {
                if (caughtException == null) {
                    caughtException = e;
                }
                getNodeEngine().getLogger(getClass()).warning("Problem while waiting for loadAll tasks to complete", e);
            }
        }
        loadAllTasks.clear();

        closeListeners();
        if (caughtException != null) {
            throw new CacheException("Problem while waiting for loadAll tasks to complete", caughtException);
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

    abstract void closeListeners();

    void ensureOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Cache operations can not be performed. The cache closed");
        }
    }

    @SuppressWarnings("unchecked")
    void submitLoadAllTask(LoadAllTask loadAllTask) {
        ExecutionService executionService = nodeEngine.getExecutionService();
        final CompletableFutureTask<Object> future =
                (CompletableFutureTask<Object>) executionService.submit("loadAll-" + nameWithPrefix, loadAllTask);
        loadAllTasks.add(future);
        future.andThen(new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                loadAllTasks.remove(future);
            }

            @Override
            public void onFailure(Throwable t) {
                loadAllTasks.remove(future);
                getNodeEngine().getLogger(getClass()).warning("Problem in loadAll task", t);
            }
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractCacheProxyBase that = (AbstractCacheProxyBase) o;
        if (nameWithPrefix != null ? !nameWithPrefix.equals(that.nameWithPrefix) : that.nameWithPrefix != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return nameWithPrefix != null ? nameWithPrefix.hashCode() : 0;
    }

    @Override
    public String toString() {
        return getClass().getName() + '{' + "name=" + name + ", nameWithPrefix=" + nameWithPrefix + '}';
    }

    final class LoadAllTask implements Runnable {

        private final CompletionListener completionListener;
        private final CacheOperationProvider operationProvider;
        private final Set<Data> keysData;
        private final boolean replaceExistingValues;

        LoadAllTask(CacheOperationProvider operationProvider, Set<Data> keysData,
                    boolean replaceExistingValues, CompletionListener completionListener) {
            this.operationProvider = operationProvider;
            this.keysData = keysData;
            this.replaceExistingValues = replaceExistingValues;
            this.completionListener = completionListener;
        }

        @Override
        public void run() {
            try {
                injectDependencies(completionListener);

                OperationService operationService = getNodeEngine().getOperationService();
                OperationFactory operationFactory;

                IPartitionService partitionService = getNodeEngine().getPartitionService();
                Map<Address, List<Integer>> memberPartitionsMap = partitionService.getMemberPartitionsMap();
                Map<Integer, Object> results = new HashMap<Integer, Object>();

                for (Map.Entry<Address, List<Integer>> memberPartitions : memberPartitionsMap.entrySet()) {
                    Set<Integer> partitions = new HashSet<Integer>(memberPartitions.getValue());
                    Set<Data> ownerKeys = filterOwnerKeys(partitionService, partitions);
                    operationFactory = operationProvider.createLoadAllOperationFactory(ownerKeys, replaceExistingValues);
                    Map<Integer, Object> memberResults;
                    memberResults = operationService.invokeOnPartitions(getServiceName(), operationFactory, partitions);
                    results.putAll(memberResults);
                }

                validateResults(results);
                if (completionListener != null) {
                    completionListener.onCompletion();
                }
            } catch (Exception e) {
                if (completionListener != null) {
                    completionListener.onException(e);
                }
            } catch (Throwable t) {
                if (t instanceof OutOfMemoryError) {
                    throw rethrow(t);
                } else {
                    if (completionListener != null) {
                        completionListener.onException(new CacheException(t));
                    }
                }
            }
        }

        private Set<Data> filterOwnerKeys(IPartitionService partitionService, Set<Integer> partitions) {
            Set<Data> ownerKeys = new HashSet<Data>();
            for (Data key : keysData) {
                int keyPartitionId = partitionService.getPartitionId(key);
                if (partitions.contains(keyPartitionId)) {
                    ownerKeys.add(key);
                }
            }
            return ownerKeys;
        }
    }
}
