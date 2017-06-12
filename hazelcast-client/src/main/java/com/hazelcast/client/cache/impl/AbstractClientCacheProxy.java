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

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.CacheGetAllCodec;
import com.hazelcast.client.impl.protocol.codec.CacheGetCodec;
import com.hazelcast.client.impl.protocol.codec.CachePutAllCodec;
import com.hazelcast.client.impl.protocol.codec.CacheSizeCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;

import javax.cache.CacheException;
import javax.cache.expiry.ExpiryPolicy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.cache.impl.CacheProxyUtil.NULL_KEY_IS_NOT_ALLOWED;
import static com.hazelcast.cache.impl.CacheProxyUtil.validateNotNull;
import static com.hazelcast.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.ExceptionUtil.rethrowAllowedTypeFirst;
import static com.hazelcast.util.MapUtil.createHashMap;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.emptyMap;

/**
 * Hazelcast provides extension functionality to default spec interface {@link javax.cache.Cache}.
 * {@link com.hazelcast.cache.ICache} is the designated interface.
 * <p>
 * AbstractCacheProxyExtension provides implementation of various {@link com.hazelcast.cache.ICache} methods.
 * <p>
 * Note: this partial implementation is used by client.
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
@SuppressWarnings("checkstyle:npathcomplexity")
abstract class AbstractClientCacheProxy<K, V> extends AbstractClientInternalCacheProxy<K, V> {

    @SuppressWarnings("unchecked")
    private static final ClientMessageDecoder CACHE_GET_RESPONSE_DECODER = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage clientMessage) {
            return (T) CacheGetCodec.decodeResponse(clientMessage).response;
        }
    };

    AbstractClientCacheProxy(CacheConfig<K, V> cacheConfig, ClientContext context) {
        super(cacheConfig, context);
    }

    protected V getSyncInternal(K key, Data keyData, ExpiryPolicy expiryPolicy) {
        long startNanos = nowInNanosOrDefault();
        try {
            ClientDelegatingFuture<V> future = getInternal(key, keyData, expiryPolicy, false);
            V value = future.get();
            if (statisticsEnabled) {
                statsHandler.onGet(startNanos, value != null);
            }
            return value;
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
        }
    }

    @Override
    public ICompletableFuture<V> getAsync(K key) {
        return getAsync(key, null);
    }

    @Override
    public ICompletableFuture<V> getAsync(K key, ExpiryPolicy expiryPolicy) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        validateNotNull(key);

        ExecutionCallback<V> callback = !statisticsEnabled ? null : statsHandler.<V>newOnGetCallback(startNanos);
        return getAsyncInternal(key, null, expiryPolicy, callback);
    }

    protected InternalCompletableFuture<V> getAsyncInternal(K key, Data keyData, ExpiryPolicy expiryPolicy,
                                                            ExecutionCallback<V> callback) {
        if (keyData == null) {
            keyData = toData(key);
        }

        ClientDelegatingFuture<V> future = getInternal(key, keyData, expiryPolicy, true);
        addCallback(future, callback);
        return future;
    }

    private ClientDelegatingFuture<V> getInternal(K key, Data keyData, ExpiryPolicy expiryPolicy, boolean deserializeResponse) {
        if (keyData == null) {
            keyData = toData(key);
        }

        Data expiryPolicyData = toData(expiryPolicy);

        ClientMessage request = CacheGetCodec.encodeRequest(nameWithPrefix, keyData, expiryPolicyData);
        int partitionId = getContext().getPartitionService().getPartitionId(keyData);

        ClientInvocation clientInvocation = new ClientInvocation(getClient(), request, partitionId);
        ClientInvocationFuture future = clientInvocation.invoke();
        return newDelegatingFuture(future, CACHE_GET_RESPONSE_DECODER, deserializeResponse);
    }

    @Override
    public ICompletableFuture<Void> putAsync(K key, V value) {
        return putAsync(key, value, null);
    }

    @Override
    public ICompletableFuture<Void> putAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return (ICompletableFuture<Void>) putAsyncInternal(key, value, expiryPolicy, false, true, newStatsCallbackOrNull(false));
    }

    @Override
    public ICompletableFuture<Boolean> putIfAbsentAsync(K key, V value) {
        return (ICompletableFuture<Boolean>) putIfAbsentSyncInternal(key, value, null, false, true);
    }

    @Override
    public ICompletableFuture<Boolean> putIfAbsentAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return (ICompletableFuture<Boolean>) putIfAbsentSyncInternal(key, value, expiryPolicy, false, true);
    }

    @Override
    public ICompletableFuture<V> getAndPutAsync(K key, V value) {
        return getAndPutAsync(key, value, null);
    }

    @Override
    public ICompletableFuture<V> getAndPutAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return putAsyncInternal(key, value, expiryPolicy, true, false, newStatsCallbackOrNull(true));
    }

    @Override
    public ICompletableFuture<Boolean> removeAsync(K key) {
        return (ICompletableFuture<Boolean>) removeAsyncInternal(key, null, false, false, true);
    }

    @Override
    public ICompletableFuture<Boolean> removeAsync(K key, V oldValue) {
        return (ICompletableFuture<Boolean>) removeAsyncInternal(key, oldValue, true, false, true);
    }

    @Override
    public ICompletableFuture<V> getAndRemoveAsync(K key) {
        return getAndRemoveAsyncInternal(key);
    }

    @Override
    public ICompletableFuture<Boolean> replaceAsync(K key, V value) {
        return replaceAsyncInternal(key, null, value, null, false, false);
    }

    @Override
    public ICompletableFuture<Boolean> replaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return replaceAsyncInternal(key, null, value, expiryPolicy, false, false);
    }

    @Override
    public ICompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
        return replaceAsyncInternal(key, oldValue, newValue, null, true, false);
    }

    @Override
    public ICompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return replaceAsyncInternal(key, oldValue, newValue, expiryPolicy, true, false);
    }

    @Override
    public ICompletableFuture<V> getAndReplaceAsync(K key, V value) {
        return replaceAndGetAsyncInternal(key, null, value, null, false, false);
    }

    @Override
    public ICompletableFuture<V> getAndReplaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        return replaceAndGetAsyncInternal(key, null, value, expiryPolicy, false, false);
    }

    @Override
    public V get(K key, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        validateNotNull(key);
        return toObject(getSyncInternal(key, null, expiryPolicy));
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys, ExpiryPolicy expiryPolicy) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        checkNotNull(keys, NULL_KEY_IS_NOT_ALLOWED);
        if (keys.isEmpty()) {
            return emptyMap();
        }

        List<Data> dataKeys = new ArrayList<Data>(keys.size());
        Map<K, V> resultMap = createHashMap(keys.size());
        getAllInternal(keys, dataKeys, expiryPolicy, resultMap, startNanos);

        return resultMap;
    }

    protected List<Map.Entry<Data, Data>> getAllInternal(Set<? extends K> keys, Collection<Data> dataKeys,
                                                         ExpiryPolicy expiryPolicy, Map<K, V> resultMap, long startNanos) {
        if (dataKeys.isEmpty()) {
            objectToDataCollection(keys, dataKeys, getSerializationService(), NULL_KEY_IS_NOT_ALLOWED);
        }
        Data expiryPolicyData = toData(expiryPolicy);

        ClientMessage request = CacheGetAllCodec.encodeRequest(nameWithPrefix, dataKeys, expiryPolicyData);
        ClientMessage responseMessage = invoke(request);

        List<Map.Entry<Data, Data>> response = CacheGetAllCodec.decodeResponse(responseMessage).response;
        for (Map.Entry<Data, Data> dataEntry : response) {
            K key = toObject(dataEntry.getKey());
            V value = toObject(dataEntry.getValue());

            resultMap.put(key, value);
        }

        if (statisticsEnabled) {
            statsHandler.onBatchGet(startNanos, response.size());
        }

        return response;
    }

    @Override
    public void put(K key, V value, ExpiryPolicy expiryPolicy) {
        putSyncInternal(key, value, expiryPolicy, false);
    }

    @Override
    public V getAndPut(K key, V value, ExpiryPolicy expiryPolicy) {
        return putSyncInternal(key, value, expiryPolicy, true);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy) {
        long startNanos = nowInNanosOrDefault();
        ensureOpen();
        checkNotNull(map, "map is null");
        if (map.isEmpty()) {
            return;
        }
        putAllInternal(map, expiryPolicy, new List[partitionCount], startNanos);
    }

    protected void putAllInternal(Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy,
                                  List<Map.Entry<Data, Data>>[] entriesPerPartition, long startNanos) {
        try {
            // first we fill entry set per partition
            groupDataToPartitions(map, getContext().getPartitionService(), entriesPerPartition);
            // then we invoke the operations and sync on completion of these operations
            putToAllPartitionsAndWaitForCompletion(entriesPerPartition, expiryPolicy, startNanos);
        } catch (Exception t) {
            throw rethrow(t);
        }
    }

    private void groupDataToPartitions(Map<? extends K, ? extends V> map, ClientPartitionService partitionService,
                                       List<Map.Entry<Data, Data>>[] entriesPerPartition) {
        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            K key = entry.getKey();
            V value = entry.getValue();
            validateNotNull(key, value);

            Data keyData = toData(key);
            Data valueData = toData(value);

            int partitionId = partitionService.getPartitionId(keyData);
            List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];
            if (entries == null) {
                entries = new ArrayList<Map.Entry<Data, Data>>();
                entriesPerPartition[partitionId] = entries;
            }

            entries.add(new QuartetMapEntry(key, keyData, value, valueData));
        }
    }

    protected class QuartetMapEntry implements Map.Entry<Data, Data> {
        private final K keyObject;
        private final V valueObject;
        private final Data keyData;
        private final Data valueData;

        public QuartetMapEntry(K keyObject, Data keyData, V valueObject, Data valueData) {
            this.keyObject = keyObject;
            this.valueObject = valueObject;
            this.keyData = keyData;
            this.valueData = valueData;
        }

        @Override
        public Data getKey() {
            return keyData;
        }

        @Override
        public Data getValue() {
            return valueData;
        }

        @Override
        public Data setValue(Data value) {
            throw new UnsupportedOperationException();
        }

        public K getKeyObject() {
            return keyObject;
        }

        public V getValueObject() {
            return valueObject;
        }
    }

    private static final class FutureEntriesTuple {

        private final Future future;
        private final List<Map.Entry<Data, Data>> entries;

        private FutureEntriesTuple(Future future, List<Map.Entry<Data, Data>> entries) {
            this.future = future;
            this.entries = entries;
        }
    }

    protected void putToAllPartitionsAndWaitForCompletion(List<Map.Entry<Data, Data>>[] entriesPerPartition,
                                                          ExpiryPolicy expiryPolicy, long startNanos)
            throws ExecutionException, InterruptedException {

        Data expiryPolicyData = toData(expiryPolicy);
        List<FutureEntriesTuple> futureEntriesTuples = new ArrayList<FutureEntriesTuple>(entriesPerPartition.length);

        for (int partitionId = 0; partitionId < entriesPerPartition.length; partitionId++) {
            List<Map.Entry<Data, Data>> entries = entriesPerPartition[partitionId];

            if (entries != null) {
                int completionId = nextCompletionId();
                // TODO: if there is a single entry, we could make use of a put operation since that is a bit cheaper
                ClientMessage request = CachePutAllCodec.encodeRequest(nameWithPrefix, entries, expiryPolicyData, completionId);
                Future future = invoke(request, partitionId, completionId);
                futureEntriesTuples.add(new FutureEntriesTuple(future, entries));
            }
        }

        waitResponseFromAllPartitionsForPutAll(futureEntriesTuples, startNanos);
    }

    private void waitResponseFromAllPartitionsForPutAll(List<FutureEntriesTuple> futureEntriesTuples, long startNanos) {
        Throwable error = null;
        for (FutureEntriesTuple tuple : futureEntriesTuples) {
            Future future = tuple.future;
            List<Map.Entry<Data, Data>> entries = tuple.entries;
            try {
                future.get();
                // Note that we count the batch put only if there is no exception while putting to target partition.
                // In case of error, some of the entries might have been put and others might fail.
                // But we simply ignore the actual put count here if there is an error.
                if (statisticsEnabled) {
                    statsHandler.getStatistics().increaseCachePuts(entries.size());
                }
            } catch (Throwable t) {
                logger.finest("Error occurred while putting entries as batch!", t);
                if (error == null) {
                    error = t;
                }
            }
        }

        if (statisticsEnabled) {
            statsHandler.getStatistics().addPutTimeNanos(nowInNanosOrDefault() - startNanos);
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
            throw rethrow(error);
        }
    }

    @Override
    public boolean putIfAbsent(K key, V value, ExpiryPolicy expiryPolicy) {
        return (Boolean) putIfAbsentSyncInternal(key, value, expiryPolicy, true, false);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return replaceSyncInternal(key, oldValue, newValue, expiryPolicy, true);
    }

    @Override
    public boolean replace(K key, V value, ExpiryPolicy expiryPolicy) {
        return replaceSyncInternal(key, null, value, expiryPolicy, false);
    }

    @Override
    public V getAndReplace(K key, V value, ExpiryPolicy expiryPolicy) {
        try {
            return toObject(replaceAndGetSyncInternal(key, null, value, expiryPolicy, false, true));
        } catch (Throwable e) {
            throw rethrowAllowedTypeFirst(e, CacheException.class);
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
            throw rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    @Override
    public CacheStatistics getLocalCacheStatistics() {
        return statsHandler.getStatistics();
    }
}
