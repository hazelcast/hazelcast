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

package com.hazelcast.client.proxy;

import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.cache.impl.CacheEventListenerAdaptor;
import com.hazelcast.cache.impl.CacheEventType;
import com.hazelcast.cache.impl.CacheProxyUtil;
import com.hazelcast.cache.impl.client.AbstractCacheRequest;
import com.hazelcast.cache.impl.client.CacheAddEntryListenerRequest;
import com.hazelcast.cache.impl.client.CacheGetAndRemoveRequest;
import com.hazelcast.cache.impl.client.CacheGetAndReplaceRequest;
import com.hazelcast.cache.impl.client.CacheLoadAllRequest;
import com.hazelcast.cache.impl.client.CachePutIfAbsentRequest;
import com.hazelcast.cache.impl.client.CachePutRequest;
import com.hazelcast.cache.impl.client.CacheRemoveEntryListenerRequest;
import com.hazelcast.cache.impl.client.CacheRemoveRequest;
import com.hazelcast.cache.impl.client.CacheReplaceRequest;
import com.hazelcast.cache.impl.operation.AbstractMutatingCacheOperation;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.nearcache.ClientHeapNearCache;
import com.hazelcast.client.nearcache.IClientNearCache;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.CompletedFuture;
import com.hazelcast.util.executor.DelegatingFuture;

import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CompletionListener;
import java.io.Closeable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;
import static com.hazelcast.cache.impl.CacheProxyUtil.validateResults;

/**
 * Base Client Cache Proxy
 */
abstract class AbstractBaseClientCacheProxy<K, V> {

    static final int TIMEOUT = 10;
    protected final ClientCacheDistributedObject delegate;
    protected final CacheConfig<K, V> cacheConfig;
    //this will represent the name from the user perspective
    protected final String name;
    protected final IClientNearCache<Data, Object> nearCache;

    private final boolean cacheOnUpdate;
    private final CopyOnWriteArrayList<Future> loadAllTasks = new CopyOnWriteArrayList<Future>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> asyncListenerRegistrations;
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> syncListenerRegistrations;
    private final ConcurrentMap<Integer, CountDownLatch> syncLocks;

    private final AtomicInteger completionIdCounter = new AtomicInteger();
    private final CacheLoader<K, V> cacheLoader;
    private final Object completionRegistrationMutex = new Object();
    private volatile String completionRegistrationId;

    protected AbstractBaseClientCacheProxy(CacheConfig cacheConfig, ClientCacheDistributedObject delegate) {
        this.name = cacheConfig.getName();
        this.cacheConfig = cacheConfig;
        this.delegate = delegate;
        if (cacheConfig.getCacheLoaderFactory() != null) {
            final Factory<CacheLoader> cacheLoaderFactory = cacheConfig.getCacheLoaderFactory();
            cacheLoader = cacheLoaderFactory.create();
        } else {
            cacheLoader = null;
        }
        asyncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        syncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        syncLocks = new ConcurrentHashMap<Integer, CountDownLatch>();

        NearCacheConfig nearCacheConfig = cacheConfig.getNearCacheConfig();
        if (nearCacheConfig != null) {
            nearCache = new ClientHeapNearCache<Data>(getDistributedObjectName(), delegate.getClientContext(), nearCacheConfig);
            cacheOnUpdate = nearCacheConfig.getLocalUpdatePolicy() == NearCacheConfig.LocalUpdatePolicy.CACHE;
        } else {
            nearCache = null;
            cacheOnUpdate = false;
        }

    }

    protected void ensureOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Cache operations can not be performed. The cache closed");
        }
    }

    protected <T> ICompletableFuture<T> invoke(ClientRequest req, Data keyData, boolean completionOperation) {

        Integer completionId = null;
        if (completionOperation) {
            completionId = registerCompletionLatch(1);
            if (req instanceof AbstractCacheRequest) {
                ((AbstractCacheRequest) req).setCompletionId(completionId);
            }
        }
        try {
            final ICompletableFuture<T> f = delegate.getClientContext().getInvocationService().invokeOnKeyOwner(req, keyData);
            if (completionOperation) {
                waitCompletionLatch(completionId);
            }
            return f;
        } catch (Throwable e) {
            if (e instanceof IllegalStateException) {
                isClosed.set(true);
            }
            if (completionOperation) {
                deregisterCompletionLatch(completionId);
            }
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    protected <T> ICompletableFuture<T> removeAsyncInternal(K key, V oldValue, boolean hasOldValue, boolean isGet,
                                                            boolean withCompletionEvent) {
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, oldValue);
        } else {
            validateNotNull(key);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key);
        }
        final Data keyData = toData(key);
        final Data oldValueData = oldValue != null ? toData(oldValue) : null;
        ClientRequest request;
        if (isGet) {
            request = new CacheGetAndRemoveRequest(getDistributedObjectName(), keyData);
        } else {
            request = new CacheRemoveRequest(getDistributedObjectName(), keyData, oldValueData);
        }
        ICompletableFuture future;
        try {
            future = invoke(request, keyData, withCompletionEvent);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new DelegatingFuture<T>(future, getContext().getSerializationService());
    }

    protected <T> ICompletableFuture<T> replaceAsyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
                                                             boolean hasOldValue, boolean isGet, boolean withCompletionEvent) {
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue, newValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, oldValue, newValue);
        } else {
            validateNotNull(key, newValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, newValue);
        }

        final Data keyData = toData(key);
        final Data oldValueData = oldValue != null ? toData(oldValue) : null;
        final Data newValueData = newValue != null ? toData(newValue) : null;
        ClientRequest request;
        if (isGet) {
            request = new CacheGetAndReplaceRequest(getDistributedObjectName(), keyData, newValueData, expiryPolicy);
        } else {
            request = new CacheReplaceRequest(getDistributedObjectName(), keyData, oldValueData, newValueData, expiryPolicy);
        }
        ICompletableFuture future;
        try {
            future = invoke(request, keyData, withCompletionEvent);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new DelegatingFuture<T>(future, getContext().getSerializationService());

    }

    protected <T> ICompletableFuture<T> putAsyncInternal(K key, V value, ExpiryPolicy expiryPolicy, boolean isGet,
                                                         boolean withCompletionEvent) {
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CachePutRequest request = new CachePutRequest(getDistributedObjectName(), keyData, valueData, expiryPolicy, isGet);
        ICompletableFuture future;
        try {
            future = invoke(request, keyData, withCompletionEvent);
            if (cacheOnUpdate) {
                storeInNearCache(keyData, valueData, value);
            } else {
                invalidateNearCache(keyData);
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return future;
    }

    protected ICompletableFuture<Boolean> putIfAbsentAsyncInternal(K key, V value, ExpiryPolicy expiryPolicy,
                                                                   boolean withCompletionEvent) {
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        final CachePutIfAbsentRequest request = new CachePutIfAbsentRequest(getDistributedObjectName(), keyData, valueData,
                expiryPolicy);
        ICompletableFuture<Boolean> future;
        try {
            future = invoke(request, keyData, withCompletionEvent);
            if (cacheOnUpdate) {
                storeInNearCache(keyData, valueData, value);
            } else {
                invalidateNearCache(keyData);
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new DelegatingFuture<Boolean>(future, getContext().getSerializationService());

    }

    protected void validateConfiguredTypes(Set<? extends K> keys)
            throws ClassCastException {
        for (K key : keys) {
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key);
        }
    }

    protected void submitLoadAllTask(final CacheLoadAllRequest request, final CompletionListener completionListener) {
        final LoadAllTask loadAllTask = new LoadAllTask(request, completionListener);
        final ClientExecutionService executionService = delegate.getClientContext().getExecutionService();

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

    protected void addListenerLocally(String regId, CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        if (cacheEntryListenerConfiguration.isSynchronous()) {
            syncListenerRegistrations.putIfAbsent(cacheEntryListenerConfiguration, regId);
            registerCompletionListener();
        } else {
            asyncListenerRegistrations.putIfAbsent(cacheEntryListenerConfiguration, regId);
        }
    }

    protected String removeListenerLocally(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        final ConcurrentMap<CacheEntryListenerConfiguration, String> regs;
        if (cacheEntryListenerConfiguration.isSynchronous()) {
            regs = syncListenerRegistrations;
        } else {
            regs = asyncListenerRegistrations;
        }
        return regs.remove(cacheEntryListenerConfiguration);
    }

    protected void validateCacheLoader(CompletionListener completionListener) {
        if (cacheLoader == null && completionListener != null) {
            completionListener.onCompletion();
        }
    }

    protected boolean isDefaultClassLoader() {
        throw new UnsupportedOperationException("not impl yet");
        //        return cacheManager.isDefaultClassLoader;
    }

    //region base cache method impls

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
        //TODO close cache issue:
        //        if(!isDefaultClassLoader()){
        delegate.destroy();
        //        }
        //close the configured CacheLoader
        closeCacheLoader();
        deregisterCompletionListener();
    }

    public boolean isClosed() {
        return isClosed.get();
    }

    //endregion

    //region DISTRIBUTED OBJECT
    public String getDistributedObjectName() {
        return delegate.getName();
    }

    protected ClientContext getContext() {
        return delegate.getClientContext();
    }

    protected void storeInNearCache(Data key, Data valueData, V value) {
        if (nearCache != null) {
            final Object valueToStore;
            if (nearCache.getInMemoryFormat() == InMemoryFormat.OBJECT) {
                valueToStore = value != null ? value : valueData;
            } else {
                valueToStore = valueData != null ? valueData : value;
            }
            nearCache.put(key, valueToStore);
        }
    }

    protected void invalidateNearCache(Data key) {
        if (nearCache != null) {
            nearCache.remove(key);
        }
    }

    protected <T> T toObject(Object data) {
        return delegate.toObject(data);
    }

    protected Data toData(Object o) {
        return delegate.toData(o);
    }

    protected void closeCacheLoader() {
        //close the configured CacheLoader
        if (cacheLoader instanceof Closeable) {
            IOUtil.closeResource((Closeable) cacheLoader);
        }
    }
    //endregion

    protected void countDownCompletionLatch(int id) {
        final CountDownLatch countDownLatch = syncLocks.get(id);
        countDownLatch.countDown();
        if (countDownLatch.getCount() == 0) {
            deregisterCompletionLatch(id);
        }
    }

    protected Integer registerCompletionLatch(int count) {
        if (!syncListenerRegistrations.isEmpty()) {
            final int id = completionIdCounter.incrementAndGet();
            CountDownLatch countDownLatch = new CountDownLatch(count);
            syncLocks.put(id, countDownLatch);
            return id;
        }
        return AbstractMutatingCacheOperation.IGNORE_COMPLETION;
    }

    protected void deregisterCompletionLatch(Integer countDownLatchId) {
        syncLocks.remove(countDownLatchId);
    }

    protected void waitCompletionLatch(Integer countDownLatchId) {
        final CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
        if (countDownLatch != null) {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
    }

    protected void waitCompletionLatch(Integer countDownLatchId, int offset) {
        //fix completion count
        final CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
        if (countDownLatch != null) {
            for (int i = 0; i < offset; i++) {
                countDownLatch.countDown();
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                ExceptionUtil.sneakyThrow(e);
            }
        }
    }

    protected void registerCompletionListener() {
        if (!syncListenerRegistrations.isEmpty() && completionRegistrationId == null) {
            synchronized (completionRegistrationMutex) {
                if (completionRegistrationId == null) {
                    final EventHandler<Object> handler = new CacheCompletionEventHandler();
                    final CacheAddEntryListenerRequest registrationRequest = new CacheAddEntryListenerRequest(
                            getDistributedObjectName());
                    completionRegistrationId = getContext().getListenerService().listen(registrationRequest, null, handler);
                }
            }
        }
    }

    protected void deregisterCompletionListener() {
        if (syncListenerRegistrations.isEmpty() && completionRegistrationId != null) {
            synchronized (completionRegistrationMutex) {
                if (completionRegistrationId != null) {
                    CacheRemoveEntryListenerRequest removeRequest = new CacheRemoveEntryListenerRequest(
                            getDistributedObjectName(), completionRegistrationId);
                    boolean isDeregistered = getContext().getListenerService()
                                                         .stopListening(removeRequest, completionRegistrationId);
                    if (isDeregistered) {
                        completionRegistrationId = null;
                    }
                }
            }
        }
    }

    protected EventHandler<Object> createHandler(final CacheEventListenerAdaptor adaptor) {
        return new EventHandler<Object>() {
            @Override
            public void handle(Object event) {
                adaptor.handleEvent(event);
            }

            @Override
            public void beforeListenerRegister() {

            }

            @Override
            public void onListenerRegister() {
            }
        };
    }

    protected ICompletableFuture createCompletedFuture(Object value) {
        return new CompletedFuture(getContext().getSerializationService(), value,
                getContext().getExecutionService().getAsyncExecutor());
    }

    private final class CacheCompletionEventHandler
            implements EventHandler<Object> {

        @Override
        public void handle(Object eventObject) {
            if (eventObject instanceof CacheEventData) {
                CacheEventData cacheEventData = (CacheEventData) eventObject;
                if (cacheEventData.getCacheEventType() == CacheEventType.COMPLETED) {
                    Integer completionId = toObject(cacheEventData.getDataValue());
                    countDownCompletionLatch(completionId);
                }
            }
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }
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
                final Map<Integer, Object> results = delegate.invoke(request);
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

}
