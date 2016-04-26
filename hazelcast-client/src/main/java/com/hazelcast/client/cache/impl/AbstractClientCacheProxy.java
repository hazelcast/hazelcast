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

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.cache.impl.ICacheInternal;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheGetAllCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutAllCodec;
import com.hazelcast.client.impl.protocol.codec.CacheSizeCodec;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;

import javax.cache.CacheException;
import javax.cache.expiry.ExpiryPolicy;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;

/**
 * <p>Hazelcast provides extension functionality to default spec interface {@link javax.cache.Cache}.
 * {@link com.hazelcast.cache.ICache} is the designated interface.</p>
 * <p>AbstractCacheProxyExtension provides implementation of various {@link com.hazelcast.cache.ICache} methods.</p>
 * <p>Note: this partial implementation is used by client.</p>
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
abstract class AbstractClientCacheProxy<K, V>
        extends AbstractClientInternalCacheProxy<K, V>
        implements ICacheInternal<K, V> {

    private static ClientMessageDecoder cacheGetResponseDecoder = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) CacheGetCodec.decodeResponse(clientMessage).response;
        }
    };

    protected AbstractClientCacheProxy(CacheConfig cacheConfig) {
        super(cacheConfig);
    }

    protected Object getFromNearCache(Data keyData, boolean async) {
        Object cached = nearCache != null ? nearCache.get(keyData) : null;
        if (cached != null && NearCache.NULL_OBJECT != cached) {
            return !async ? cached : createCompletedFuture(cached);
        }
        return null;
    }

    protected Object getInternal(K key, ExpiryPolicy expiryPolicy, boolean async) {
        final long start = System.nanoTime();
        ensureOpen();
        validateNotNull(key);
        final Data keyData = toData(key);
        Object cached = getFromNearCache(keyData, async);
        if (cached != null) {
            return cached;
        }
        final Data expiryPolicyData = toData(expiryPolicy);
        ClientMessage request = CacheGetCodec.encodeRequest(nameWithPrefix, keyData, expiryPolicyData);
        ClientInvocationFuture future;
        try {
            final int partitionId = clientContext.getPartitionService().getPartitionId(key);
            final HazelcastClientInstanceImpl client = (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
            final ClientInvocation clientInvocation = new ClientInvocation(client, request, partitionId);
            future = clientInvocation.invoke();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        SerializationService serializationService = clientContext.getSerializationService();
        ClientDelegatingFuture<V> delegatingFuture =
                new ClientDelegatingFuture<V>(future, serializationService, cacheGetResponseDecoder);
        if (async) {
            if (nearCache != null) {
                delegatingFuture.andThenInternal(new ExecutionCallback<Data>() {
                    public void onResponse(Data valueData) {
                        storeInNearCache(keyData, valueData, null);
                        if (statisticsEnabled) {
                            handleStatisticsOnGet(start, valueData);
                        }
                    }

                    public void onFailure(Throwable t) {
                    }
                });
            }
            return delegatingFuture;
        } else {
            try {
                Object value = toObject(delegatingFuture.get());
                if (nearCache != null) {
                    storeInNearCache(keyData, (Data) delegatingFuture.getResponse(), (V) value);
                }
                if (statisticsEnabled) {
                    handleStatisticsOnGet(start, value);
                }
                return value;
            } catch (Throwable e) {
                throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
            }
        }
    }

    protected void handleStatisticsOnGet(long start, Object response) {
        if (response == null) {
            statistics.increaseCacheMisses();
        } else {
            statistics.increaseCacheHits();
        }
        statistics.addGetTimeNanos(System.nanoTime() - start);
    }

    @Override
    public ICompletableFuture<V> getAsync(K key) {
        return getAsync(key, null);
    }

    @Override
    public ICompletableFuture<V> getAsync(K key, ExpiryPolicy expiryPolicy) {
        return (ICompletableFuture<V>) getInternal(key, expiryPolicy, true);
    }

    @Override
    public ICompletableFuture<Void> putAsync(K key, V value) {
        return putAsync(key, value, null);
    }

    @Override
    public ICompletableFuture<Void> putAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return (ICompletableFuture<Void>) putInternal(key, value, expiryPolicy, false, true, true);
    }

    @Override
    public ICompletableFuture<Boolean> putIfAbsentAsync(K key, V value) {
        return (ICompletableFuture<Boolean>) putIfAbsentInternal(key, value, null, false, true);
    }

    @Override
    public ICompletableFuture<Boolean> putIfAbsentAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return (ICompletableFuture<Boolean>) putIfAbsentInternal(key, value, expiryPolicy, false, true);
    }

    @Override
    public ICompletableFuture<V> getAndPutAsync(K key, V value) {
        return getAndPutAsync(key, value, null);
    }

    @Override
    public ICompletableFuture<V> getAndPutAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return (ICompletableFuture<V>) putInternal(key, value, expiryPolicy, true, false, true);
    }

    @Override
    public ICompletableFuture<Boolean> removeAsync(K key) {
        return removeAsyncInternal(key, null, false, false, true);
    }

    @Override
    public ICompletableFuture<Boolean> removeAsync(K key, V oldValue) {
        return removeAsyncInternal(key, oldValue, true, false, true);
    }

    @Override
    public ICompletableFuture<V> getAndRemoveAsync(K key) {
        return getAndRemoveAsyncInternal(key, false, true);
    }

    @Override
    public ICompletableFuture<Boolean> replaceAsync(K key, V value) {
        return replaceInternal(key, null, value, null, false, false, true);
    }

    @Override
    public ICompletableFuture<Boolean> replaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return replaceInternal(key, null, value, expiryPolicy, false, false, true);
    }

    @Override
    public ICompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
        return replaceInternal(key, oldValue, newValue, null, true, false, true);
    }

    @Override
    public ICompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return replaceInternal(key, oldValue, newValue, expiryPolicy, true, false, true);
    }

    @Override
    public ICompletableFuture<V> getAndReplaceAsync(K key, V value) {
        return replaceAndGetAsyncInternal(key, null, value, null, false, false, true);
    }

    @Override
    public ICompletableFuture<V> getAndReplaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return replaceAndGetAsyncInternal(key, null, value, expiryPolicy, false, false, true);
    }

    @Override
    public V get(K key, ExpiryPolicy expiryPolicy) {
        return (V) getInternal(key, expiryPolicy, false);
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys, ExpiryPolicy expiryPolicy) {
        final long start = System.nanoTime();
        ensureOpen();
        validateNotNull(keys);
        if (keys.isEmpty()) {
            return Collections.EMPTY_MAP;
        }
        final Set<Data> keySet = new HashSet<Data>(keys.size());
        for (K key : keys) {
            final Data k = toData(key);
            keySet.add(k);
        }
        Map<K, V> result = getAllFromNearCache(keySet);
        if (keySet.isEmpty()) {
            return result;
        }
        Data expiryPolicyData = toData(expiryPolicy);
        ClientMessage request = CacheGetAllCodec.encodeRequest(nameWithPrefix, keySet, expiryPolicyData);
        ClientMessage responseMessage = invoke(request);
        List<Map.Entry<Data, Data>> entries = CacheGetAllCodec.decodeResponse(responseMessage).response;
        for (Map.Entry<Data, Data> dataEntry : entries) {
            Data keyData = dataEntry.getKey();
            Data valueData = dataEntry.getValue();
            K key = toObject(keyData);
            V value = toObject(valueData);
            result.put(key, value);
            storeInNearCache(keyData, valueData, value);
        }
        if (statisticsEnabled) {
            statistics.increaseCacheHits(entries.size());
            statistics.addGetTimeNanos(System.nanoTime() - start);
        }
        return result;
    }

    private Map<K, V> getAllFromNearCache(Set<Data> keySet) {
        Map<K, V> result = new HashMap<K, V>();
        if (nearCache != null) {
            final Iterator<Data> iterator = keySet.iterator();
            while (iterator.hasNext()) {
                Data key = iterator.next();
                Object cached = nearCache.get(key);
                if (cached != null && !NearCache.NULL_OBJECT.equals(cached)) {
                    result.put((K) toObject(key), (V) cached);
                    iterator.remove();
                }
            }
        }
        return result;
    }

    @Override
    public void put(K key, V value, ExpiryPolicy expiryPolicy) {
        putInternal(key, value, expiryPolicy, false, true, false);
    }

    @Override
    public V getAndPut(K key, V value, ExpiryPolicy expiryPolicy) {
        return (V) putInternal(key, value, expiryPolicy, true, true, false);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy) {
        final long start = System.nanoTime();

        ensureOpen();
        validateNotNull(map);

        ClientPartitionService partitionService = clientContext.getPartitionService();
        int partitionCount = partitionService.getPartitionCount();

        try {
            // First we fill entry set per partition
            List<Map.Entry<Data, Data>>[] entriesPerPartition =
                    groupDataToPartitions(map, partitionService, partitionCount);

            // Then we invoke the operations and sync on completion of these operations
            putToAllPartitionsAndWaitForCompletion(entriesPerPartition, expiryPolicy, start);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private List<Map.Entry<Data, Data>>[] groupDataToPartitions(Map<? extends K, ? extends V> map,
                                                                ClientPartitionService partitionService,
                                                                int partitionCount) {
        List<Map.Entry<Data, Data>>[] entriesPerPartition = new List[partitionCount];
        SerializationService serializationService = clientContext.getSerializationService();

        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            K key = entry.getKey();
            V value = entry.getValue();
            validateNotNull(key, value);

            Data keyData = serializationService.toData(key);
            Data valueData = serializationService.toData(value);

            int partitionId = partitionService.getPartitionId(keyData);
            List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];
            if (entries == null) {
                entries = new ArrayList<Map.Entry<Data, Data>>();
                entriesPerPartition[partitionId] = entries;
            }

            entries.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(keyData, valueData));
        }

        return entriesPerPartition;
    }

    private static final class FutureEntriesTuple {

        private Future future;
        private List<Map.Entry<Data, Data>> entries;

        private FutureEntriesTuple(Future future, List<Map.Entry<Data, Data>> entries) {
            this.future = future;
            this.entries = entries;
        }

    }

    private void putToAllPartitionsAndWaitForCompletion(List<Map.Entry<Data, Data>>[] entriesPerPartition,
                                                        ExpiryPolicy expiryPolicy, long start)
            throws ExecutionException, InterruptedException {
        Data expiryPolicyData = toData(expiryPolicy);
        List<FutureEntriesTuple> futureEntriesTuples =
                new ArrayList<FutureEntriesTuple>(entriesPerPartition.length);
        for (int partitionId = 0; partitionId < entriesPerPartition.length; partitionId++) {
            List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];
            if (entries != null) {
                int completionId = nextCompletionId();
                // TODO If there is a single entry, we could make use of a put operation since that is a bit cheaper
                ClientMessage request =
                        CachePutAllCodec.encodeRequest(nameWithPrefix, entries, expiryPolicyData, completionId);
                Future f = invoke(request, partitionId, completionId);
                futureEntriesTuples.add(new FutureEntriesTuple(f, entries));
            }
        }

        waitResponseFromAllPartitionsForPutAll(futureEntriesTuples, start);
    }

    private void waitResponseFromAllPartitionsForPutAll(List<FutureEntriesTuple> futureEntriesTuples,
                                                        long start) {
        Throwable error = null;
        for (FutureEntriesTuple tuple : futureEntriesTuples) {
            Future future = tuple.future;
            List<Map.Entry<Data, Data>> entries = tuple.entries;
            try {
                future.get();
                if (nearCache != null) {
                    handleNearCacheOnPutAll(entries, !cacheOnUpdate);
                }
                // Note that we count the batch put only if there is no exception while putting to target partition.
                // In case of error, some of the entries might have been put and others might fail.
                // But we simply ignore the actual put count here if there is an error.
                if (statisticsEnabled) {
                    statistics.increaseCachePuts(entries.size());
                }
            } catch (Throwable t) {
                if (nearCache != null) {
                    handleNearCacheOnPutAll(entries, true);
                }
                logger.finest("Error occurred while putting entries as batch!", t);
                if (error == null) {
                    error = t;
                }
            }
        }

        if (statisticsEnabled) {
            statistics.addPutTimeNanos(System.nanoTime() - start);
        }

        if (error != null) {
            /*
             * There maybe multiple exceptions but we throw only the first one.
             * There are some ideas to throw all exceptions to caller but all of them have drawbacks:
             *      - `Thread::addSuppressed` can be used to add other exceptions to the first one
             *        but it is available since JDK 7.
             *      - `Thread::initCause` can be used but this is wrong as semantic
             *        since the other exceptions are not cause of the first one.
             *      - We may wrap all exceptions in our custom exception (such as `MultipleCacheException`)
             *        but in this case caller may wait different exception type and this idea causes problem.
             *        For example see this TCK test:
             *              `org.jsr107.tck.integration.CacheWriterTest::shouldWriteThoughUsingPutAll_partialSuccess`
             *        In this test exception is thrown at `CacheWriter` and caller side expects this exception.
             * So as a result, we only throw the first exception and others are suppressed by only logging.
             */
            ExceptionUtil.rethrow(error);
        }
    }

    private void handleNearCacheOnPutAll(List<Map.Entry<Data, Data>> entries, boolean invalidate) {
        if (nearCache != null) {
            if (cacheOnUpdate) {
                for (Map.Entry<Data, Data> entry : entries) {
                    storeInNearCache(entry.getKey(), entry.getValue(), null);
                }
            } else {
                for (Map.Entry<Data, Data> entry : entries) {
                    invalidateNearCache(entry.getKey());
                }
            }
        }
    }

    @Override
    public boolean putIfAbsent(K key, V value, ExpiryPolicy expiryPolicy) {
        return (Boolean) putIfAbsentInternal(key, value, expiryPolicy, true, false);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        final long start = System.nanoTime();
        final Future<Boolean> f = replaceInternal(key, oldValue, newValue, expiryPolicy, true, true, false);
        try {
            boolean replaced = f.get();
            if (statisticsEnabled) {
                handleStatisticsOnReplace(false, start, replaced);
            }
            return replaced;
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public boolean replace(K key, V value, ExpiryPolicy expiryPolicy) {
        final long start = System.nanoTime();
        final Future<Boolean> f = replaceInternal(key, null, value, expiryPolicy, false, true, false);
        try {
            boolean replaced = f.get();
            if (statisticsEnabled) {
                handleStatisticsOnReplace(false, start, replaced);
            }
            return replaced;
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public V getAndReplace(K key, V value, ExpiryPolicy expiryPolicy) {
        final long start = System.nanoTime();
        final Future<V> f = replaceAndGetAsyncInternal(key, null, value, expiryPolicy, false, true, false);
        try {
            V oldValue = f.get();
            if (statisticsEnabled) {
                handleStatisticsOnReplace(true, start, oldValue);
            }
            return oldValue;
        } catch (Throwable e) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public int size() {
        ensureOpen();
        try {
            ClientMessage request = CacheSizeCodec.encodeRequest(nameWithPrefix);
            ClientMessage resultMessage = invoke(request);
            return CacheSizeCodec.decodeResponse(resultMessage).response;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    @Override
    public CacheStatistics getLocalCacheStatistics() {
        return statistics;
    }

}
