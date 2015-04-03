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

import com.hazelcast.cache.impl.CacheClearResponse;
import com.hazelcast.cache.impl.CacheEventListenerAdaptor;
import com.hazelcast.cache.impl.CacheProxyUtil;
import com.hazelcast.cache.impl.CacheSyncListenerCompleter;
import com.hazelcast.cache.impl.client.AbstractCacheRequest;
import com.hazelcast.cache.impl.client.CacheAddInvalidationListenerRequest;
import com.hazelcast.cache.impl.client.CacheClearRequest;
import com.hazelcast.cache.impl.client.CacheGetAndRemoveRequest;
import com.hazelcast.cache.impl.client.CacheGetAndReplaceRequest;
import com.hazelcast.cache.impl.client.CacheInvalidationMessage;
import com.hazelcast.cache.impl.client.CachePutIfAbsentRequest;
import com.hazelcast.cache.impl.client.CachePutRequest;
import com.hazelcast.cache.impl.client.CacheRemoveEntryListenerRequest;
import com.hazelcast.cache.impl.client.CacheRemoveInvalidationListenerRequest;
import com.hazelcast.cache.impl.client.CacheRemoveRequest;
import com.hazelcast.cache.impl.client.CacheReplaceRequest;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.CompletedFuture;
import com.hazelcast.util.executor.DelegatingFuture;

import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;

/**
 * Abstract {@link com.hazelcast.cache.ICache} implementation which provides shared internal implementations
 * of cache operations like put, replace, remove and invoke. These internal implementations are delegated
 * by actual cache methods.
 * <p/>
 * <p>Note: this partial implementation is used by client.</p>
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
abstract class AbstractClientInternalCacheProxy<K, V>
        extends AbstractClientCacheProxyBase<K, V> implements CacheSyncListenerCompleter {

    private static final long MAX_COMPLETION_LATCH_WAIT_TIME = TimeUnit.MINUTES.toMillis(5);
    private static final long COMPLETION_LATCH_WAIT_TIME_STEP = TimeUnit.SECONDS.toMillis(1);

    protected final ILogger logger = Logger.getLogger(getClass());

    protected final HazelcastClientCacheManager cacheManager;
    protected final NearCacheManager nearCacheManager;
    // Object => Data or <V>
    protected NearCache<Data, Object> nearCache;

    protected String nearCacheMembershipRegistrationId;
    protected final ConcurrentMap<Member, String> nearCacheInvalidationListeners =
        new ConcurrentHashMap<Member, String>();

    private final boolean cacheOnUpdate;
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> asyncListenerRegistrations;
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> syncListenerRegistrations;
    private final ConcurrentMap<Integer, CountDownLatch> syncLocks;

    private final AtomicInteger completionIdCounter = new AtomicInteger();

    protected AbstractClientInternalCacheProxy(CacheConfig cacheConfig, ClientContext clientContext,
                                               HazelcastClientCacheManager cacheManager) {
        super(cacheConfig, clientContext);
        this.cacheManager = cacheManager;
        nearCacheManager = clientContext.getNearCacheManager();
        asyncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        syncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        syncLocks = new ConcurrentHashMap<Integer, CountDownLatch>();

        NearCacheConfig nearCacheConfig = cacheConfig.getNearCacheConfig();
        if (nearCacheConfig != null) {
            cacheOnUpdate = nearCacheConfig.getLocalUpdatePolicy() == NearCacheConfig.LocalUpdatePolicy.CACHE;
            NearCacheContext nearCacheContext =
                    new NearCacheContext(clientContext.getSerializationService(),
                            createNearCacheExecutor(clientContext.getExecutionService()));
            nearCache = nearCacheManager
                    .getOrCreateNearCache(nameWithPrefix, nearCacheConfig, nearCacheContext);
            registerInvalidationListener();
        } else {
            nearCache = null;
            cacheOnUpdate = false;
        }
    }

    protected class ClientNearCacheExecutor implements Executor {

        protected ClientExecutionService clientExecutionService;

        protected ClientNearCacheExecutor(ClientExecutionService clientExecutionService) {
            this.clientExecutionService = clientExecutionService;
        }

        @Override
        public void execute(Runnable runnable) {
            clientExecutionService.execute(runnable);
        }

    }

    protected Executor createNearCacheExecutor(ClientExecutionService clientExecutionService) {
        return new ClientNearCacheExecutor(clientExecutionService);
    }

    @Override
    public void close() {
        if (nearCache != null) {
            removeInvalidationListener();
            nearCacheManager.clearNearCache(nearCache.getName());
        }
        super.close();
    }

    @Override
    public void destroy() {
        if (nearCache != null) {
            removeInvalidationListener();
            nearCacheManager.destroyNearCache(nearCache.getName());
            nearCache = null;
        }
        super.destroy();
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
            int partitionId = clientContext.getPartitionService().getPartitionId(keyData);
            HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
            final ClientInvocation clientInvocation = new ClientInvocation(client, req, partitionId);
            final ICompletableFuture<T> f = clientInvocation.invoke();
            if (completionOperation) {
                waitCompletionLatch(completionId);
            }
            return f;
        } catch (Throwable e) {
            if (e instanceof IllegalStateException) {
                close();
            }
            if (completionOperation) {
                deregisterCompletionLatch(completionId);
            }
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    protected <T> T getSafely(Future<T> future) {
        try {
            return future.get();
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    //region internal base operations
    protected <T> ICompletableFuture<T> removeAsyncInternal(K key, V oldValue, boolean hasOldValue,
                                                            boolean isGet, boolean withCompletionEvent) {
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
        InMemoryFormat inMemoryFormat = cacheConfig.getInMemoryFormat();
        if (isGet) {
            request = new CacheGetAndRemoveRequest(nameWithPrefix, keyData, inMemoryFormat);
        } else {
            request = new CacheRemoveRequest(nameWithPrefix, keyData, oldValueData, inMemoryFormat);
        }
        ICompletableFuture future;
        try {
            future = invoke(request, keyData, withCompletionEvent);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new DelegatingFuture<T>(future, clientContext.getSerializationService());
    }

    protected <T> ICompletableFuture<T> replaceAsyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
                                                             boolean hasOldValue, boolean isGet,
                                                             boolean withCompletionEvent) {
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
        InMemoryFormat inMemoryFormat = cacheConfig.getInMemoryFormat();
        ClientRequest request;
        if (isGet) {
            request = new CacheGetAndReplaceRequest(nameWithPrefix, keyData, newValueData,
                    expiryPolicy, inMemoryFormat);
        } else {
            request = new CacheReplaceRequest(nameWithPrefix, keyData, oldValueData, newValueData,
                    expiryPolicy, inMemoryFormat);
        }
        ICompletableFuture future;
        try {
            future = invoke(request, keyData, withCompletionEvent);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new DelegatingFuture<T>(future, clientContext.getSerializationService());
    }

    protected <T> ICompletableFuture<T> putAsyncInternal(K key, V value, ExpiryPolicy expiryPolicy,
                                                         boolean isGet, boolean withCompletionEvent) {
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        InMemoryFormat inMemoryFormat = cacheConfig.getInMemoryFormat();
        CachePutRequest request = new CachePutRequest(nameWithPrefix, keyData, valueData,
                expiryPolicy, isGet, inMemoryFormat);
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
        CachePutIfAbsentRequest request = new CachePutIfAbsentRequest(nameWithPrefix, keyData, valueData,
                expiryPolicy, cacheConfig.getInMemoryFormat());
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
        return new DelegatingFuture<Boolean>(future, clientContext.getSerializationService());
    }

    protected void removeAllInternal(Set<? extends K> keys, boolean isRemoveAll) {
        final Set<Data> keysData;
        if (keys != null) {
            keysData = new HashSet<Data>();
            for (K key : keys) {
                keysData.add(toData(key));
            }
        } else {
            keysData = null;
        }
        final int partitionCount = clientContext.getPartitionService().getPartitionCount();
        int completionId = registerCompletionLatch(partitionCount);
        CacheClearRequest request = new CacheClearRequest(nameWithPrefix, keysData, isRemoveAll, completionId);
        try {
            final Map<Integer, Object> results = invoke(request);
            int completionCount = 0;
            for (Object result : results.values()) {
                if (result != null && result instanceof CacheClearResponse) {
                    final Object response = ((CacheClearResponse) result).getResponse();
                    if (response instanceof Boolean) {
                        completionCount++;
                    }
                    if (response instanceof Throwable) {
                        throw (Throwable) response;
                    }
                }
            }
            waitCompletionLatch(completionId, partitionCount - completionCount);
        } catch (Throwable t) {
            deregisterCompletionLatch(completionId);
            throw ExceptionUtil.rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    protected void clearInternal() {
        CacheClearRequest request = new CacheClearRequest(nameWithPrefix, null, false, -1);
        try {
            final Map<Integer, Object> results = invoke(request);
            for (Object result : results.values()) {
                if (result != null && result instanceof CacheClearResponse) {
                    final Object response = ((CacheClearResponse) result).getResponse();
                    if (response instanceof Throwable) {
                        throw (Throwable) response;
                    }
                }
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    protected void storeInNearCache(Data key, Data valueData, V value) {
        if (nearCache != null) {
            Object valueToStore = nearCache.selectToSave(value, valueData);
            nearCache.put(key, valueToStore);
        }
    }

    protected void invalidateNearCache(Data key) {
        if (nearCache != null) {
            nearCache.invalidate(key);
        }
    }
    //endregion internal base operations

    protected void addListenerLocally(String regId,
                                      CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        if (cacheEntryListenerConfiguration.isSynchronous()) {
            syncListenerRegistrations.putIfAbsent(cacheEntryListenerConfiguration, regId);
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

    private void deregisterAllCacheEntryListener(Collection<String> listenerRegistrations) {
        for (String regId : listenerRegistrations) {
            CacheRemoveEntryListenerRequest removeReq =
                    new CacheRemoveEntryListenerRequest(nameWithPrefix, regId);
            clientContext.getListenerService().stopListening(removeReq, regId);
        }
    }

    @Override
    protected void closeListeners() {
        deregisterAllCacheEntryListener(syncListenerRegistrations.values());
        deregisterAllCacheEntryListener(asyncListenerRegistrations.values());

        syncListenerRegistrations.clear();
        asyncListenerRegistrations.clear();
        notifyAndClearSyncListenerLatches();
    }

    private void notifyAndClearSyncListenerLatches() {
        // notify waiting sync listeners
        Collection<CountDownLatch> latches = syncLocks.values();
        Iterator<CountDownLatch> iterator = latches.iterator();
        while (iterator.hasNext()) {
            CountDownLatch latch = iterator.next();
            iterator.remove();
            while (latch.getCount() > 0) {
                latch.countDown();
            }
        }
    }

    public void countDownCompletionLatch(int id) {
        final CountDownLatch countDownLatch = syncLocks.get(id);
        if (countDownLatch == null) {
            return;
        }
        countDownLatch.countDown();
        if (countDownLatch.getCount() == 0) {
            deregisterCompletionLatch(id);
        }
    }

    protected Integer registerCompletionLatch(int count) {
        if (!syncListenerRegistrations.isEmpty()) {
            final int id = completionIdCounter.incrementAndGet();
            int size = syncListenerRegistrations.size();
            CountDownLatch countDownLatch = new CountDownLatch(count * size);
            syncLocks.put(id, countDownLatch);
            return id;
        }
        return MutableOperation.IGNORE_COMPLETION;
    }

    protected void deregisterCompletionLatch(Integer countDownLatchId) {
        syncLocks.remove(countDownLatchId);
    }

    protected void waitCompletionLatch(Integer countDownLatchId) {
        final CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
        if (countDownLatch != null) {
            awaitLatch(countDownLatch);
        }
    }

    protected void waitCompletionLatch(Integer countDownLatchId, int offset) {
        //fix completion count
        final CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
        if (countDownLatch != null) {
            for (int i = 0; i < offset; i++) {
                countDownLatch.countDown();
            }
            awaitLatch(countDownLatch);
        }
    }

    private void awaitLatch(CountDownLatch countDownLatch) {
        try {
            long currentTimeoutMs = MAX_COMPLETION_LATCH_WAIT_TIME;
            // Call latch await in small steps to be able to check if node is still active.
            // If not active then throw HazelcastInstanceNotActiveException,
            // If closed or destroyed then throw IllegalStateException,
            // otherwise continue to wait until `MAX_COMPLETION_LATCH_WAIT_TIME` passes.
            //
            // Warning: Silently ignoring if latch does not countDown in time.
            while (currentTimeoutMs > 0
                    && !countDownLatch.await(COMPLETION_LATCH_WAIT_TIME_STEP, TimeUnit.MILLISECONDS)) {
                currentTimeoutMs -= COMPLETION_LATCH_WAIT_TIME_STEP;
                if (!clientContext.isActive()) {
                    throw new HazelcastInstanceNotActiveException();
                } else if (isClosed()) {
                    throw new IllegalStateException("Cache (" + nameWithPrefix + ") is closed !");
                } else if (isDestroyed()) {
                    throw new IllegalStateException("Cache (" + nameWithPrefix + ") is destroyed !");
                }
            }
        } catch (InterruptedException e) {
            ExceptionUtil.sneakyThrow(e);
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
        return new CompletedFuture(clientContext.getSerializationService(), value,
                clientContext.getExecutionService().getAsyncExecutor());
    }

    protected final class NearCacheMembershipListener implements MembershipListener {

        public void memberAdded(MembershipEvent event) {
            MemberImpl member = (MemberImpl) event.getMember();
            addInvalidationListener(member);
        }

        public void memberRemoved(MembershipEvent event) {
            MemberImpl member = (MemberImpl) event.getMember();
            removeInvalidationListener(member, false);
        }

        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {

        }

    }

    protected final class NearCacheInvalidationHandler implements EventHandler<CacheInvalidationMessage> {

        private final Client client;

        private NearCacheInvalidationHandler(Client client) {
            this.client = client;
        }

        @Override
        public void handle(CacheInvalidationMessage message) {
            if (client.getUuid().equals(message.getSourceUuid())) {
                return;
            }
            Data key = message.getKey();
            if (key != null) {
                nearCache.invalidate(key);
            } else {
                nearCache.clear();
            }
        }

        @Override
        public void beforeListenerRegister() {

        }

        @Override
        public void onListenerRegister() {
            nearCache.clear();
        }

    }

    protected void registerInvalidationListener() {
        if (nearCache != null && nearCache.isInvalidateOnChange()) {
            ClientClusterService clusterService = clientContext.getClusterService();
            nearCacheMembershipRegistrationId =
                    clusterService.addMembershipListener(new NearCacheMembershipListener());
            Collection<MemberImpl> memberList = clusterService.getMemberList();
            for (MemberImpl member : memberList) {
                addInvalidationListener(member);
            }
        }
    }

    protected void removeInvalidationListener() {
        if (nearCache != null && nearCache.isInvalidateOnChange()) {
            String registrationId = nearCacheMembershipRegistrationId;
            ClientClusterService clusterService = clientContext.getClusterService();
            if (registrationId != null) {
                clusterService.removeMembershipListener(registrationId);
            }
            Collection<MemberImpl> memberList = clusterService.getMemberList();
            for (MemberImpl member : memberList) {
                removeInvalidationListener(member, true);
            }
        }
    }

    protected void addInvalidationListener(MemberImpl member) {
        if (nearCacheInvalidationListeners.containsKey(member)) {
            return;
        }
        try {
            ClientRequest request = new CacheAddInvalidationListenerRequest(nameWithPrefix);
            Client client = clientContext.getClusterService().getLocalClient();
            EventHandler handler = new NearCacheInvalidationHandler(client);
            HazelcastClientInstanceImpl clientInstance = (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
            ClientInvocation invocation = new ClientInvocation(clientInstance, handler, request, member.getAddress());
            Future future = invocation.invoke();
            String registrationId = clientContext.getSerializationService().toObject(future.get());
            clientContext.getListenerService().registerListener(registrationId, request.getCallId());

            nearCacheInvalidationListeners.put(member, registrationId);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    protected void removeInvalidationListener(MemberImpl member, boolean removeFromMemberAlso) {
        String registrationId = nearCacheInvalidationListeners.remove(member);
        if (registrationId != null) {
            try {
                if (removeFromMemberAlso) {
                    ClientRequest request = new CacheRemoveInvalidationListenerRequest(nameWithPrefix, registrationId);
                    HazelcastClientInstanceImpl clientInstance =
                            (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
                    ClientInvocation invocation = new ClientInvocation(clientInstance, request, member.getAddress());
                    Future future = invocation.invoke();
                    Boolean result = clientContext.getSerializationService().toObject(future.get());
                    if (!result) {
                        logger.warning("Invalidation listener couldn't be removed on member " + member.getAddress());
                        // TODO What we do if result is false ???
                    }
                }
                clientContext.getListenerService().deRegisterListener(registrationId);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
    }

}
