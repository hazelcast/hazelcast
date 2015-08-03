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

import com.hazelcast.cache.impl.CacheEventData;
import com.hazelcast.cache.impl.CacheEventListenerAdaptor;
import com.hazelcast.cache.impl.CacheProxyUtil;
import com.hazelcast.cache.impl.CacheSyncListenerCompleter;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheExecutor;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheAddInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheClearCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetAndRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetAndReplaceCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveAllCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveAllKeysCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheRemoveInvalidationListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CacheReplaceCodec;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientListenerService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientListenerInvocation;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.spi.impl.ListenerRemoveCodec;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Client;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.CompletedFuture;

import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
        extends AbstractClientCacheProxyBase<K, V>
        implements CacheSyncListenerCompleter {

    private static final long MAX_COMPLETION_LATCH_WAIT_TIME = TimeUnit.MINUTES.toMillis(5);
    private static final long COMPLETION_LATCH_WAIT_TIME_STEP = TimeUnit.SECONDS.toMillis(1);

    private static final ClientMessageDecoder getAndRemoveResponseDecoder = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) CacheGetAndRemoveCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder removeResponseDecoder = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) CacheRemoveCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder replaceResponseDecoder = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) CacheReplaceCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder getAndReplaceResponseDecoder = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) CacheGetAndReplaceCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder putResponseDecoder = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) CachePutCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder putIfAbsentResponseDecoder = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) Boolean.valueOf(CachePutIfAbsentCodec.decodeResponse(clientMessage).response);
        }
    };

    protected final ILogger logger = Logger.getLogger(getClass());

    protected final HazelcastClientCacheManager cacheManager;
    protected final NearCacheManager nearCacheManager;
    // Object => Data or <V>
    protected NearCache<Data, Object> nearCache;

    protected String nearCacheMembershipRegistrationId;
    protected final ConcurrentMap<Member, String> nearCacheInvalidationListeners = new ConcurrentHashMap<Member, String>();

    private boolean cacheOnUpdate;
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> asyncListenerRegistrations;
    private final ConcurrentMap<CacheEntryListenerConfiguration, String> syncListenerRegistrations;
    private final ConcurrentMap<Integer, CountDownLatch> syncLocks;


    protected AbstractClientInternalCacheProxy(CacheConfig cacheConfig, ClientContext clientContext,
                                               HazelcastClientCacheManager cacheManager) {
        super(cacheConfig, clientContext);
        this.cacheManager = cacheManager;
        nearCacheManager = clientContext.getNearCacheManager();
        asyncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        syncListenerRegistrations = new ConcurrentHashMap<CacheEntryListenerConfiguration, String>();
        syncLocks = new ConcurrentHashMap<Integer, CountDownLatch>();

        initNearCache();
    }

    private void initNearCache() {
        NearCacheConfig nearCacheConfig = clientContext.getClientConfig().getNearCacheConfig(name);
        if (nearCacheConfig != null) {
            cacheOnUpdate = nearCacheConfig.getLocalUpdatePolicy() == NearCacheConfig.LocalUpdatePolicy.CACHE;
            NearCacheContext nearCacheContext =
                    new NearCacheContext(nearCacheManager,
                            clientContext.getSerializationService(),
                            createNearCacheExecutor(clientContext.getExecutionService()));
            nearCache = nearCacheManager.getOrCreateNearCache(nameWithPrefix, nearCacheConfig, nearCacheContext);
            registerInvalidationListener();
        }
    }

    private static class ClientNearCacheExecutor
            implements NearCacheExecutor {

        private ClientExecutionService clientExecutionService;

        private ClientNearCacheExecutor(ClientExecutionService clientExecutionService) {
            this.clientExecutionService = clientExecutionService;
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
            return clientExecutionService.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        }

    }

    private NearCacheExecutor createNearCacheExecutor(ClientExecutionService clientExecutionService) {
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
        }
        super.destroy();
    }

    protected ClientInvocationFuture invoke(ClientMessage req, Data keyData, int completionId) {
        final boolean completionOperation = completionId != -1;
        if (completionOperation) {
            registerCompletionLatch(completionId, 1);
        }
        try {
            int partitionId = clientContext.getPartitionService().getPartitionId(keyData);
            HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
            final ClientInvocation clientInvocation = new ClientInvocation(client, req, partitionId);
            ClientInvocationFuture f = clientInvocation.invoke();
            if (completionOperation) {
                waitCompletionLatch(completionId, f);
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
    protected <T> ICompletableFuture<T> getAndRemoveAsyncInternal(K key, boolean withCompletionEvent) {
        ensureOpen();
        validateNotNull(key);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key);
        final Data keyData = toData(key);
        final int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request;
        request = CacheGetAndRemoveCodec.encodeRequest(nameWithPrefix, keyData, completionId);
        ClientInvocationFuture future;
        try {
            future = invoke(request, keyData, completionId);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new ClientDelegatingFuture<T>(future, clientContext.getSerializationService(),
                getAndRemoveResponseDecoder);
    }

    protected <T> ICompletableFuture<T> removeAsyncInternal(K key, V oldValue, boolean hasOldValue,
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
        final Data oldValueData = toData(oldValue);
        final int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request;
        request = CacheRemoveCodec.encodeRequest(nameWithPrefix, keyData, oldValueData, completionId);
        ClientInvocationFuture future;
        try {
            future = invoke(request, keyData, completionId);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new ClientDelegatingFuture<T>(future, clientContext.getSerializationService(),
                removeResponseDecoder);
    }

    protected <T> ICompletableFuture<T> replaceAsyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
                                                             boolean hasOldValue, boolean withCompletionEvent) {
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue, newValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, oldValue, newValue);
        } else {
            validateNotNull(key, newValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, newValue);
        }

        final Data keyData = toData(key);
        final Data oldValueData = toData(oldValue);
        final Data newValueData = toData(newValue);
        final Data expiryPolicyData = toData(expiryPolicy);
        final int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CacheReplaceCodec.encodeRequest(nameWithPrefix, keyData, oldValueData, newValueData,
                expiryPolicyData, completionId);
        ClientInvocationFuture future;
        try {
            future = invoke(request, keyData, completionId);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }

        return new ClientDelegatingFuture<T>(future, clientContext.getSerializationService(),
                replaceResponseDecoder);
    }

    protected <T> ICompletableFuture<T> replaceAndGetAsyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy,
                                                                   boolean hasOldValue, boolean withCompletionEvent) {
        ensureOpen();
        if (hasOldValue) {
            validateNotNull(key, oldValue, newValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, oldValue, newValue);
        } else {
            validateNotNull(key, newValue);
            CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, newValue);
        }

        final Data keyData = toData(key);
        final Data newValueData = toData(newValue);
        final Data expiryPolicyData = toData(expiryPolicy);
        final int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CacheGetAndReplaceCodec.encodeRequest(nameWithPrefix, keyData, newValueData,
                expiryPolicyData, completionId);
        ClientInvocationFuture future;
        try {
            future = invoke(request, keyData, completionId);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }

        return new ClientDelegatingFuture<T>(future, clientContext.getSerializationService(),
                getAndReplaceResponseDecoder);
    }

    protected <T> ICompletableFuture<T> putAsyncInternal(K key, V value, ExpiryPolicy expiryPolicy, boolean isGet,
                                                         boolean withCompletionEvent) {
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        final Data expiryPolicyData = toData(expiryPolicy);
        final int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CachePutCodec.encodeRequest(nameWithPrefix, keyData,
                valueData, expiryPolicyData, isGet, completionId);
        ClientInvocationFuture future;
        try {
            future = invoke(request, keyData, completionId);
            if (cacheOnUpdate) {
                storeInNearCache(keyData, valueData, value);
            } else {
                invalidateNearCache(keyData);
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }

        return new ClientDelegatingFuture<T>(future, clientContext.getSerializationService(), putResponseDecoder);
    }

    protected ICompletableFuture<Boolean> putIfAbsentAsyncInternal(K key, V value, ExpiryPolicy expiryPolicy,
                                                                   boolean withCompletionEvent) {
        ensureOpen();
        validateNotNull(key, value);
        CacheProxyUtil.validateConfiguredTypes(cacheConfig, key, value);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        final Data expiryPolicyData = toData(expiryPolicy);
        final int completionId = withCompletionEvent ? nextCompletionId() : -1;
        ClientMessage request = CachePutIfAbsentCodec.encodeRequest(nameWithPrefix, keyData,
                valueData, expiryPolicyData, completionId);
        ClientInvocationFuture future;
        try {
            future = invoke(request, keyData, completionId);
            if (cacheOnUpdate) {
                storeInNearCache(keyData, valueData, value);
            } else {
                invalidateNearCache(keyData);
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new ClientDelegatingFuture<Boolean>(future, clientContext.getSerializationService(),
                putIfAbsentResponseDecoder);
    }

    protected void removeAllKeysInternal(Set<? extends K> keys) {
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
        final int completionId = nextCompletionId();
        registerCompletionLatch(completionId, partitionCount);
        ClientMessage request = CacheRemoveAllKeysCodec.encodeRequest(nameWithPrefix, keysData, completionId);
        try {
            invoke(request);
            waitCompletionLatch(completionId, null);
        } catch (Throwable t) {
            deregisterCompletionLatch(completionId);
            throw ExceptionUtil.rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    protected void removeAllInternal() {
        final int partitionCount = clientContext.getPartitionService().getPartitionCount();
        final int completionId = nextCompletionId();
        registerCompletionLatch(completionId, partitionCount);
        ClientMessage request = CacheRemoveAllCodec.encodeRequest(nameWithPrefix, completionId);
        try {
            invoke(request);
            waitCompletionLatch(completionId, null);
        } catch (Throwable t) {
            deregisterCompletionLatch(completionId);
            throw ExceptionUtil.rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    protected void clearInternal() {
        ClientMessage request = CacheClearCodec.encodeRequest(nameWithPrefix);
        try {
            invoke(request);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    protected void storeInNearCache(Data key, Data valueData, V value) {
        if (nearCache != null && valueData != null) {
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

    protected void addListenerLocally(String regId, CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
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
        ClientListenerService listenerService = clientContext.getListenerService();

        ListenerRemoveCodec listenerRemoveCodec = new ListenerRemoveCodec() {
            @Override
            public ClientMessage encodeRequest(String realRegistrationId) {
                return CacheRemoveEntryListenerCodec.encodeRequest(nameWithPrefix, realRegistrationId);
            }

            @Override
            public boolean decodeResponse(ClientMessage clientMessage) {
                return CacheRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
        for (String regId : listenerRegistrations) {
            listenerService.stopListening(regId, listenerRemoveCodec);
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

    protected Integer registerCompletionLatch(Integer countDownLatchId, int count) {
        if (!syncListenerRegistrations.isEmpty()) {
            int size = syncListenerRegistrations.size();
            CountDownLatch countDownLatch = new CountDownLatch(count * size);
            syncLocks.put(countDownLatchId, countDownLatch);
            return countDownLatchId;
        }
        return MutableOperation.IGNORE_COMPLETION;
    }

    protected void deregisterCompletionLatch(Integer countDownLatchId) {
        syncLocks.remove(countDownLatchId);
    }

    protected void waitCompletionLatch(Integer countDownLatchId, ICompletableFuture future)
            throws ExecutionException {
        final CountDownLatch countDownLatch = syncLocks.get(countDownLatchId);
        if (countDownLatch != null) {
            awaitLatch(countDownLatch, future);
        }
    }

    private void awaitLatch(CountDownLatch countDownLatch, ICompletableFuture future)
            throws ExecutionException {
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
                if (future != null && future.isDone()) {
                    Object response = future.get();
                    if (response instanceof Throwable) {
                        return;
                    }
                }
                currentTimeoutMs -= COMPLETION_LATCH_WAIT_TIME_STEP;
                if (!clientContext.isActive()) {
                    throw new HazelcastInstanceNotActiveException();
                } else if (isClosed()) {
                    throw new IllegalStateException("Cache (" + nameWithPrefix + ") is closed !");
                } else if (isDestroyed()) {
                    throw new IllegalStateException("Cache (" + nameWithPrefix + ") is destroyed !");
                }
            }
            if (countDownLatch.getCount() > 0) {
                logger.finest("Countdown latch wait timeout after "
                        + MAX_COMPLETION_LATCH_WAIT_TIME + " milliseconds!");
            }
        } catch (InterruptedException e) {
            ExceptionUtil.sneakyThrow(e);
        }
    }

    protected EventHandler createHandler(CacheEventListenerAdaptor<K, V> adaptor) {
        return new CacheEventHandler(adaptor);
    }

    private class CacheEventHandler extends CacheAddEntryListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final CacheEventListenerAdaptor<K, V> adaptor;

        private CacheEventHandler(CacheEventListenerAdaptor<K, V> adaptor) {
            this.adaptor = adaptor;
        }

        @Override
        public void handle(int type, Set<CacheEventData> keys, int completionId) {
            adaptor.handle(type, keys, completionId);
        }

        @Override
        public void beforeListenerRegister() {

        }

        @Override
        public void onListenerRegister() {
        }
    }

    protected ICompletableFuture createCompletedFuture(Object value) {
        return new CompletedFuture(clientContext.getSerializationService(), value,
                clientContext.getExecutionService().getAsyncExecutor());
    }

    private class NearCacheMembershipListener
            implements MembershipListener {

        @Override
        public void memberAdded(MembershipEvent event) {
            Member member = event.getMember();
            addInvalidationListener(member);
        }

        @Override
        public void memberRemoved(MembershipEvent event) {
            Member member = event.getMember();
            removeInvalidationListener(member, false);
        }

        @Override
        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {

        }

    }

    private class NearCacheInvalidationHandler extends CacheAddInvalidationListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final Client client;

        private NearCacheInvalidationHandler(Client client) {
            this.client = client;
        }

        @Override
        public void handle(String name, Data key, String sourceUuid) {
            if (client.getUuid().equals(sourceUuid)) {
                return;
            }
            if (key != null) {
                nearCache.invalidate(key);
            } else {
                nearCache.clear();
            }
        }

        @Override
        public void handle(String name, List<Data> keys, List<String> sourceUuids) {
            if (sourceUuids != null && !sourceUuids.isEmpty()) {
                Iterator<Data> keysIt = keys.iterator();
                Iterator<String> sourceUuidsIt = sourceUuids.iterator();
                while (keysIt.hasNext() && sourceUuidsIt.hasNext()) {
                    Data key = keysIt.next();
                    String sourceUuid = sourceUuidsIt.next();
                    if (!client.getUuid().equals(sourceUuid)) {
                        nearCache.invalidate(key);
                    }
                }
            } else {
                for (Data key : keys) {
                    nearCache.invalidate(key);
                }
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

    private void registerInvalidationListener() {
        if (nearCache != null && nearCache.isInvalidateOnChange()) {
            ClientClusterService clusterService = clientContext.getClusterService();
            nearCacheMembershipRegistrationId =
                    clusterService.addMembershipListener(new NearCacheMembershipListener());
            Collection<Member> memberList = clusterService.getMemberList();
            for (Member member : memberList) {
                addInvalidationListener(member);
            }
        }
    }

    private void removeInvalidationListener() {
        if (nearCache != null && nearCache.isInvalidateOnChange()) {
            String registrationId = nearCacheMembershipRegistrationId;
            ClientClusterService clusterService = clientContext.getClusterService();
            if (registrationId != null) {
                clusterService.removeMembershipListener(registrationId);
            }
            Collection<Member> memberList = clusterService.getMemberList();
            for (Member member : memberList) {
                removeInvalidationListener(member, true);
            }
        }
    }

    private void addInvalidationListener(Member member) {
        if (nearCacheInvalidationListeners.containsKey(member)) {
            return;
        }
        try {
            ClientMessage request = CacheAddInvalidationListenerCodec.encodeRequest(nameWithPrefix);
            Client client = clientContext.getClusterService().getLocalClient();
            EventHandler handler = new NearCacheInvalidationHandler(client);
            HazelcastClientInstanceImpl clientInstance = (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
            Address address = member.getAddress();
            ClientInvocation invocation = new ClientListenerInvocation(clientInstance, handler, request, address,
                    new ClientMessageDecoder() {
                        @Override
                        public <T> T decodeClientMessage(ClientMessage clientMessage) {
                            return (T) CacheAddInvalidationListenerCodec.decodeResponse(clientMessage).response;
                        }
                    });
            Future<ClientMessage> future = invocation.invoke();
            String registrationId = CacheAddInvalidationListenerCodec.decodeResponse(future.get()).response;
            clientContext.getListenerService().registerListener(registrationId, request.getCorrelationId());

            nearCacheInvalidationListeners.put(member, registrationId);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private void removeInvalidationListener(Member member, boolean removeFromMemberAlso) {
        String registrationId = nearCacheInvalidationListeners.remove(member);
        if (registrationId == null) {
            return;
        }

        Address address = member.getAddress();
        if (!removeFromMemberAlso) {
            return;
        }
        boolean result = clientContext.getListenerService().stopListening(registrationId, new ListenerRemoveCodec() {
            @Override
            public ClientMessage encodeRequest(String realRegistrationId) {
                return CacheRemoveInvalidationListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeResponse(ClientMessage clientMessage) {
                return CacheRemoveInvalidationListenerCodec.decodeResponse(clientMessage).response;
            }
        });

        if (!result) {
            logger.warning("Invalidation listener couldn't be removed on member " + address);
        }

    }

}
