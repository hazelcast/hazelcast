/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.proxy;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import com.hazelcast.internal.journal.EventJournalReader;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.ComputeEntryProcessor;
import com.hazelcast.map.impl.ComputeIfAbsentEntryProcessor;
import com.hazelcast.map.impl.ComputeIfPresentEntryProcessor;
import com.hazelcast.map.impl.KeyValueConsumingEntryProcessor;
import com.hazelcast.map.impl.MapEntryReplacingEntryProcessor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MergeEntryProcessor;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.iterator.MapIterable;
import com.hazelcast.map.impl.iterator.MapIterator;
import com.hazelcast.map.impl.iterator.MapPartitionIterable;
import com.hazelcast.map.impl.iterator.MapPartitionIterator;
import com.hazelcast.map.impl.iterator.MapQueryIterable;
import com.hazelcast.map.impl.iterator.MapQueryPartitionIterable;
import com.hazelcast.map.impl.iterator.MapQueryPartitionIterator;
import com.hazelcast.map.impl.journal.MapEventJournalReadOperation;
import com.hazelcast.map.impl.journal.MapEventJournalSubscribeOperation;
import com.hazelcast.map.impl.query.AggregationResult;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.Target;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheEndToEndProvider;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequest;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContext;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.Preconditions.checkNoNullInside;
import static com.hazelcast.internal.util.Preconditions.checkNotInstanceOf;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.internal.util.SetUtil.createHashSet;
import static com.hazelcast.internal.util.TimeUtil.timeInMsOrTimeIfNullUnit;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.query.QueryResultUtils.transformToSet;
import static com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequest.newQueryCacheRequest;
import static com.hazelcast.map.impl.record.Record.UNSET;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newDelegatingFuture;
import static java.util.Collections.emptyMap;

/**
 * Proxy implementation of {@link IMap} interface.
 *
 * @param <K> the key type of map.
 * @param <V> the value type of map.
 */
@SuppressWarnings({"checkstyle:classfanoutcomplexity", "checkstyle:ClassDataAbstractionCoupling"})
public class MapProxyImpl<K, V> extends MapProxySupport<K, V> implements EventJournalReader<EventJournalMapEvent<K, V>> {

    public MapProxyImpl(String name, MapService mapService, NodeEngine nodeEngine, MapConfig mapConfig) {
        super(name, mapService, nodeEngine, mapConfig);
    }

    @Override
    public V get(@Nonnull Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return toObject(getInternal(key));
    }

    @Override
    public V put(@Nonnull K key, @Nonnull V value) {
        return put(key, value, UNSET, TimeUnit.MILLISECONDS);
    }

    @Override
    public V put(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        Data result = putInternal(key, valueData, ttl, timeunit, UNSET, TimeUnit.MILLISECONDS);
        return toObject(result);
    }

    @Override
    public V put(@Nonnull K key, @Nonnull V value,
                 long ttl, @Nonnull TimeUnit ttlUnit,
                 long maxIdle, @Nonnull TimeUnit maxIdleUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(ttlUnit, NULL_TTL_UNIT_IS_NOT_ALLOWED);
        checkNotNull(maxIdleUnit, NULL_MAX_IDLE_UNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        Data result = putInternal(key, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit);
        return toObject(result);
    }

    @Override
    public boolean tryPut(@Nonnull K key, @Nonnull V value, long timeout, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return tryPutInternal(key, valueData, timeout, timeunit);
    }

    @Override
    public V putIfAbsent(@Nonnull K key, @Nonnull V value) {
        return putIfAbsent(key, value, UNSET, TimeUnit.MILLISECONDS);
    }

    @Override
    public V putIfAbsent(@Nonnull K key, @Nonnull V value,
                         long ttl, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        Data result = putIfAbsentInternal(key, valueData, ttl, timeunit, UNSET, TimeUnit.MILLISECONDS);
        return toObject(result);
    }

    @Override
    public V putIfAbsent(@Nonnull K key, @Nonnull V value,
                         long ttl, @Nonnull TimeUnit timeunit,
                         long maxIdle, @Nonnull TimeUnit maxIdleUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);
        checkNotNull(maxIdleUnit, NULL_MAX_IDLE_UNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        Data result = putIfAbsentInternal(key, valueData, ttl, timeunit, maxIdle, maxIdleUnit);
        return toObject(result);
    }

    @Override
    public void putTransient(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        putTransientInternal(key, valueData, ttl, timeunit, UNSET, TimeUnit.MILLISECONDS);
    }

    @Override
    public void putTransient(@Nonnull K key, @Nonnull V value,
                             long ttl, @Nonnull TimeUnit ttlUnit,
                             long maxIdle, @Nonnull TimeUnit maxIdleUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(ttlUnit, NULL_TTL_UNIT_IS_NOT_ALLOWED);
        checkNotNull(maxIdleUnit, NULL_MAX_IDLE_UNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        putTransientInternal(key, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit);
    }

    @Override
    public boolean replace(@Nonnull K key, @Nonnull V oldValue, @Nonnull V newValue) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(oldValue, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(newValue, NULL_VALUE_IS_NOT_ALLOWED);

        Data oldValueData = toData(oldValue);
        Data newValueData = toData(newValue);
        return replaceInternal(key, oldValueData, newValueData);
    }

    @Override
    public V replace(@Nonnull K key, @Nonnull V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return toObject(replaceInternal(key, valueData));
    }

    @Override
    public void set(@Nonnull K key, @Nonnull V value) {
        set(key, value, UNSET, TimeUnit.MILLISECONDS);
    }

    @Override
    public void set(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit ttlUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(ttlUnit, NULL_TTL_UNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        setInternal(key, valueData, ttl, ttlUnit, UNSET, TimeUnit.MILLISECONDS);
    }

    @Override
    public void set(@Nonnull K key, @Nonnull V value,
                    long ttl, @Nonnull TimeUnit ttlUnit,
                    long maxIdle, @Nonnull TimeUnit maxIdleUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(ttlUnit, NULL_TTL_UNIT_IS_NOT_ALLOWED);
        checkNotNull(maxIdleUnit, NULL_MAX_IDLE_UNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        setInternal(key, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit);
    }

    @Override
    public V remove(@Nonnull Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data result = removeInternal(key);
        return toObject(result);
    }

    @Override
    public boolean remove(@Nonnull Object key, @Nonnull Object value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return removeInternal(key, valueData);
    }

    @Override
    public void removeAll(@Nonnull Predicate<K, V> predicate) {
        checkNotNull(predicate, "predicate cannot be null");
        handleHazelcastInstanceAwareParams(predicate);

        removeAllInternal(predicate);
    }

    @Override
    public void delete(@Nonnull Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        deleteInternal(key);
    }

    @Override
    public boolean containsKey(@Nonnull Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return containsKeyInternal(key);
    }

    @Override
    public boolean containsValue(@Nonnull Object value) {
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return containsValueInternal(valueData);
    }

    @Override
    public void lock(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toDataWithStrategy(key);
        lockSupport.lock(getNodeEngine(), keyData);
    }

    @Override
    public void lock(@Nonnull Object key, long leaseTime, @Nullable TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkPositive("leaseTime", leaseTime);

        Data keyData = toDataWithStrategy(key);
        lockSupport.lock(getNodeEngine(), keyData, timeInMsOrTimeIfNullUnit(leaseTime, timeUnit));
    }

    @Override
    public void unlock(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toDataWithStrategy(key);
        lockSupport.unlock(getNodeEngine(), keyData);
    }

    @Override
    public boolean tryRemove(@Nonnull K key, long timeout, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        return tryRemoveInternal(key, timeout, timeunit);
    }

    @Override
    public InternalCompletableFuture<V> getAsync(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return newDelegatingFuture(serializationService, getAsyncInternal(key));
    }

    @Override
    public boolean isLocked(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toDataWithStrategy(key);
        return lockSupport.isLocked(getNodeEngine(), keyData);
    }

    @Override
    public InternalCompletableFuture<V> putAsync(@Nonnull K key, @Nonnull V value) {
        return putAsync(key, value, UNSET, TimeUnit.MILLISECONDS);
    }

    @Override
    public InternalCompletableFuture<V> putAsync(@Nonnull K key, @Nonnull V value,
                                                 long ttl, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return newDelegatingFuture(
                serializationService,
                putAsyncInternal(key, valueData, ttl, timeunit, UNSET, TimeUnit.MILLISECONDS));
    }

    @Override
    public InternalCompletableFuture<V> putAsync(@Nonnull K key, @Nonnull V value,
                                                 long ttl, @Nonnull TimeUnit ttlUnit,
                                                 long maxIdle, @Nonnull TimeUnit maxIdleUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(ttlUnit, NULL_TTL_UNIT_IS_NOT_ALLOWED);
        checkNotNull(maxIdleUnit, NULL_MAX_IDLE_UNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return newDelegatingFuture(
                serializationService,
                putAsyncInternal(key, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit));
    }

    public InternalCompletableFuture<V> putIfAbsentAsync(@Nonnull K key, @Nonnull V value) {
        return putIfAbsentAsync(key, value, UNSET, TimeUnit.MILLISECONDS);
    }

    public InternalCompletableFuture<V> putIfAbsentAsync(@Nonnull K key, @Nonnull V value,
                                                    long ttl, @Nonnull TimeUnit timeunit) {
        return putIfAbsentAsync(key, value, ttl, timeunit, UNSET, TimeUnit.MILLISECONDS);
    }

    public InternalCompletableFuture<V> putIfAbsentAsync(@Nonnull K key, @Nonnull V value,
                                                         long ttl, @Nonnull TimeUnit timeunit,
                                                         long maxIdle, @Nonnull TimeUnit maxIdleUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);
        checkNotNull(maxIdleUnit, NULL_MAX_IDLE_UNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return newDelegatingFuture(
                serializationService,
                putIfAbsentAsyncInternal(key, valueData, ttl, timeunit, maxIdle, maxIdleUnit));
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(@Nonnull K key, @Nonnull V value) {
        return setAsync(key, value, UNSET, TimeUnit.MILLISECONDS);
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return newDelegatingFuture(
                serializationService,
                setAsyncInternal(key, valueData, ttl, timeunit, UNSET, TimeUnit.MILLISECONDS));
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(@Nonnull K key,
                                                    @Nonnull V value,
                                                    long ttl, @Nonnull TimeUnit ttlUnit,
                                                    long maxIdle, @Nonnull TimeUnit maxIdleUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(ttlUnit, NULL_TTL_UNIT_IS_NOT_ALLOWED);
        checkNotNull(maxIdleUnit, NULL_MAX_IDLE_UNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return newDelegatingFuture(
                serializationService,
                setAsyncInternal(key, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit));
    }

    @Override
    public InternalCompletableFuture<V> removeAsync(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return newDelegatingFuture(serializationService, removeAsyncInternal(key));
    }

    @Override
    public Map<K, V> getAll(@Nullable Set<K> keys) {
        if (CollectionUtil.isEmpty(keys)) {
            // Wrap emptyMap() into unmodifiableMap to make sure put/putAll methods throw UnsupportedOperationException
            return Collections.unmodifiableMap(emptyMap());
        }

        int keysSize = keys.size();
        List<Data> dataKeys = new LinkedList<>();
        List<Object> resultingKeyValuePairs = new ArrayList<>(keysSize * 2);
        getAllInternal(keys, dataKeys, resultingKeyValuePairs);

        Map<K, V> result = createHashMap(keysSize);
        for (int i = 0; i < resultingKeyValuePairs.size(); ) {
            K key = toObject(resultingKeyValuePairs.get(i++));
            V value = toObject(resultingKeyValuePairs.get(i++));
            result.put(key, value);
        }
        return Collections.unmodifiableMap(result);
    }

    @Override
    public boolean setTtl(@Nonnull K key, long ttl, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        return setTtlInternal(key, ttl, timeunit);
    }

    @Override
    public void putAll(@Nonnull Map<? extends K, ? extends V> map) {
        checkNotNull(map, "Null argument map is not allowed");
        putAllInternal(map, null, true);
    }

    @Override
    public InternalCompletableFuture<Void> putAllAsync(@Nonnull Map<? extends K, ? extends V> map) {
        checkNotNull(map, "Null argument map is not allowed");
        InternalCompletableFuture<Void> future = new InternalCompletableFuture<>();
        putAllInternal(map, future, true);
        return future;
    }

    @Override
    public void setAll(@Nonnull Map<? extends K, ? extends V> map) {
        checkNotNull(map, "Null argument map is not allowed");
        putAllInternal(map, null, false);
    }

    @Override
    public InternalCompletableFuture<Void> setAllAsync(@Nonnull Map<? extends K, ? extends V> map) {
        checkNotNull(map, "Null argument map is not allowed");
        InternalCompletableFuture<Void> future = new InternalCompletableFuture<>();
        putAllInternal(map, future, false);
        return future;
    }

    @Override
    public boolean tryLock(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toDataWithStrategy(key);
        return lockSupport.tryLock(getNodeEngine(), keyData);
    }

    @Override
    public boolean tryLock(@Nonnull K key, long time, @Nullable TimeUnit timeunit) throws InterruptedException {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toDataWithStrategy(key);
        return lockSupport.tryLock(getNodeEngine(), keyData, time, timeunit);
    }

    @Override
    public boolean tryLock(@Nonnull K key,
                           long time, @Nullable TimeUnit timeunit,
                           long leaseTime, @Nullable TimeUnit leaseTimeUnit)
            throws InterruptedException {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toDataWithStrategy(key);
        return lockSupport.tryLock(getNodeEngine(), keyData, time, timeunit, leaseTime, leaseTimeUnit);
    }

    @Override
    public void forceUnlock(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toDataWithStrategy(key);
        lockSupport.forceUnlock(getNodeEngine(), keyData);
    }

    @Override
    public String addInterceptor(@Nonnull MapInterceptor interceptor) {
        checkNotNull(interceptor, "Interceptor should not be null!");
        handleHazelcastInstanceAwareParams(interceptor);

        return addMapInterceptorInternal(interceptor);
    }

    @Override
    public boolean removeInterceptor(@Nonnull String id) {
        checkNotNull(id, "Interceptor ID should not be null!");

        return removeMapInterceptorInternal(id);
    }

    @Override
    public UUID addLocalEntryListener(@Nonnull MapListener listener) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addLocalEntryListenerInternal(listener);
    }

    @Override
    public UUID addLocalEntryListener(@Nonnull MapListener listener,
                                      @Nonnull Predicate<K, V> predicate,
                                      boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addLocalEntryListenerInternal(listener, predicate, null, includeValue);
    }

    @Override
    public UUID addLocalEntryListener(@Nonnull MapListener listener,
                                      @Nonnull Predicate<K, V> predicate,
                                      @Nullable K key,
                                      boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addLocalEntryListenerInternal(listener, predicate, toDataWithStrategy(key), includeValue);
    }

    @Override
    public UUID addEntryListener(@Nonnull MapListener listener, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addEntryListenerInternal(listener, null, includeValue);
    }

    @Override
    public UUID addEntryListener(@Nonnull MapListener listener, @Nonnull K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addEntryListenerInternal(listener, toDataWithStrategy(key), includeValue);
    }

    @Override
    public UUID addEntryListener(@Nonnull MapListener listener,
                                 @Nonnull Predicate<K, V> predicate,
                                 @Nullable K key,
                                 boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addEntryListenerInternal(listener, predicate, toDataWithStrategy(key), includeValue);
    }

    @Override
    public UUID addEntryListener(@Nonnull MapListener listener,
                                 @Nonnull Predicate<K, V> predicate,
                                 boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addEntryListenerInternal(listener, predicate, null, includeValue);
    }

    @Override
    public boolean removeEntryListener(@Nonnull UUID id) {
        checkNotNull(id, "Listener ID should not be null!");

        return removeEntryListenerInternal(id);
    }

    @Override
    public UUID addPartitionLostListener(@Nonnull MapPartitionLostListener listener) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addPartitionLostListenerInternal(listener);
    }

    @Override
    public boolean removePartitionLostListener(@Nonnull UUID id) {
        checkNotNull(id, "Listener ID should not be null!");

        return removePartitionLostListenerInternal(id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public EntryView<K, V> getEntryView(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        SimpleEntryView<K, V> entryViewInternal = (SimpleEntryView<K, V>) getEntryViewInternal(toDataWithStrategy(key));
        if (entryViewInternal == null) {
            return null;
        }
        Data value = (Data) entryViewInternal.getValue();
        entryViewInternal.setKey(key);
        entryViewInternal.setValue(toObject(value));
        return entryViewInternal;
    }

    @Override
    public boolean evict(@Nonnull Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return evictInternal(key);
    }

    @Override
    public void evictAll() {
        evictAllInternal();
    }

    @Override
    public void loadAll(boolean replaceExistingValues) {
        checkTrue(isMapStoreEnabled(), "First you should configure a map store");

        loadAllInternal(replaceExistingValues);
    }

    @Override
    public void loadAll(@Nonnull Set<K> keys, boolean replaceExistingValues) {
        checkNotNull(keys, NULL_KEYS_ARE_NOT_ALLOWED);
        checkNoNullInside(keys, NULL_KEY_IS_NOT_ALLOWED);
        checkTrue(isMapStoreEnabled(), "First you should configure a map store");

        loadInternal(keys, null, replaceExistingValues);
    }

    @Override
    public void clear() {
        clearInternal();
    }

    @Nonnull
    @Override
    public Set<K> keySet() {
        return keySet(Predicates.alwaysTrue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet(@Nonnull Predicate<K, V> predicate) {
        return executePredicate(predicate, IterationType.KEY, true, Target.ALL_NODES);
    }

    /**
     * Execute the {@code keySet} operation only on the given {@code
     * partitions}.
     * <p>
     * Performance note: the query will use non-partitioned indexes. If not all
     * partitions of a member are in the set, the index will return entries for
     * all the partitions, but those will be subsequently eliminated. If all
     * (or most) partitions of a member are in the set, the performance hit is
     * small.
     * <p>
     * <b>Warning:</b> {@code partitions} is mutated during the call.
     */
    @SuppressWarnings("unchecked")
    public Set<K> keySet(@Nonnull Predicate<K, V> predicate, PartitionIdSet partitions) {
        return executePredicate(predicate, IterationType.KEY, true, Target.createPartitionTarget(partitions));
    }

    @Nonnull
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return entrySet(Predicates.alwaysTrue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<Map.Entry<K, V>> entrySet(@Nonnull Predicate predicate) {
        return executePredicate(predicate, IterationType.ENTRY, true, Target.ALL_NODES);
    }

    /**
     * Execute the {@code entrySet} operation only on the given {@code
     * partitions}.
     * <p>
     * Performance note: the query will use non-partitioned indexes. If not all
     * partitions of a member are in the set, the index will return entries for
     * all the partitions, but those will be subsequently eliminated. If all
     * (or most) partitions of a member are in the set, the performance hit is
     * small.
     * <p>
     * <b>Warning:</b> {@code partitions} is mutated during the call.
     */
    @SuppressWarnings("unchecked")
    public Set<Map.Entry<K, V>> entrySet(@Nonnull Predicate predicate, PartitionIdSet partitions) {
        return executePredicate(predicate, IterationType.ENTRY, true, Target.createPartitionTarget(partitions));
    }

    @Nonnull
    @Override
    public Collection<V> values() {
        return values(Predicates.alwaysTrue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<V> values(@Nonnull Predicate predicate) {
        return executePredicate(predicate, IterationType.VALUE, false, Target.ALL_NODES);
    }

    /**
     * Execute the {@code values} operation only on the given {@code
     * partitions}.
     * <p>
     * Performance note: the query will use non-partitioned indexes. If not all
     * partitions of a member are in the set, the index will return entries for
     * all the partitions, but those will be subsequently eliminated. If all
     * (or most) partitions of a member are in the set, the performance hit is
     * small.
     * <p>
     * <b>Warning:</b> {@code partitions} is mutated during the call.
     */
    @SuppressWarnings("unchecked")
    public Collection<V> values(@Nonnull Predicate predicate, PartitionIdSet partitions) {
        return executePredicate(predicate, IterationType.VALUE, false, Target.createPartitionTarget(partitions));
    }

    private Set executePredicate(Predicate predicate, IterationType iterationType, boolean uniqueResult, Target target) {
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        QueryResult result = executeQueryInternal(predicate, iterationType, target);
        incrementOtherOperationsStat();
        return transformToSet(serializationService, result, predicate, iterationType, uniqueResult, false);
    }

    @Override
    public Set<K> localKeySet() {
        return localKeySet(Predicates.alwaysTrue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> localKeySet(@Nonnull Predicate predicate) {
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        QueryResult result = executeQueryInternal(predicate, IterationType.KEY, Target.LOCAL_NODE);
        incrementOtherOperationsStat();
        return transformToSet(serializationService, result, predicate, IterationType.KEY, false, false);
    }

    @Override
    public <R> R executeOnKey(@Nonnull K key,
                              @Nonnull EntryProcessor<K, V, R> entryProcessor) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(entryProcessor);

        Data result = executeOnKeyInternal(key, entryProcessor).joinInternal();
        return toObject(result);
    }

    @Override
    public <R> Map<K, R> executeOnKeys(@Nonnull Set<K> keys,
                                       @Nonnull EntryProcessor<K, V, R> entryProcessor) {
        try {
            return submitToKeys(keys, entryProcessor).get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public <R> InternalCompletableFuture<Map<K, R>> submitToKeys(@Nonnull Set<K> keys,
                                                                 @Nonnull EntryProcessor<K, V, R> entryProcessor) {
        checkNotNull(keys, NULL_KEYS_ARE_NOT_ALLOWED);
        if (keys.isEmpty()) {
            return newCompletedFuture(Collections.emptyMap());
        }
        handleHazelcastInstanceAwareParams(entryProcessor);

        Set<Data> dataKeys = createHashSet(keys.size());
        return submitToKeysInternal(keys, dataKeys, entryProcessor);
    }

    @Override
    public <R> InternalCompletableFuture<R> submitToKey(@Nonnull K key,
                                                        @Nonnull EntryProcessor<K, V, R> entryProcessor) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(entryProcessor);

        InternalCompletableFuture<Data> future = executeOnKeyInternal(key, entryProcessor);
        return newDelegatingFuture(serializationService, future);
    }

    @Override
    public <R> Map<K, R> executeOnEntries(@Nonnull EntryProcessor<K, V, R> entryProcessor) {
        return executeOnEntries(entryProcessor, Predicates.alwaysTrue());
    }

    @Override
    public <R> Map<K, R> executeOnEntries(@Nonnull EntryProcessor<K, V, R> entryProcessor,
                                          @Nonnull Predicate<K, V> predicate) {
        handleHazelcastInstanceAwareParams(entryProcessor, predicate);
        List<Data> result = new ArrayList<>();

        executeOnEntriesInternal(entryProcessor, predicate, result);
        if (result.isEmpty()) {
            return emptyMap();
        }

        Map<K, R> resultingMap = createHashMap(result.size() / 2);
        for (int i = 0; i < result.size(); ) {
            Data key = result.get(i++);
            Data value = result.get(i++);

            resultingMap.put(toObject(key), toObject(value));

        }
        return resultingMap;
    }

    @Override
    public <R> R aggregate(@Nonnull Aggregator<? super Map.Entry<K, V>, R> aggregator) {
        return aggregate(aggregator, Predicates.alwaysTrue());
    }

    @Override
    public <R> R aggregate(@Nonnull Aggregator<? super Map.Entry<K, V>, R> aggregator,
                           @Nonnull Predicate<K, V> predicate) {
        checkNotNull(aggregator, NULL_AGGREGATOR_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        checkNotPagingPredicate(predicate, "aggregate");

        // HazelcastInstanceAware handled by cloning
        aggregator = serializationService.toObject(serializationService.toData(aggregator));

        AggregationResult result = executeQueryInternal(predicate, aggregator, null, IterationType.ENTRY, Target.ALL_NODES);
        return result.<R>getAggregator().aggregate();
    }

    @Override
    public <R> Collection<R> project(@Nonnull Projection<? super Map.Entry<K, V>, R> projection) {
        return project(projection, Predicates.alwaysTrue());
    }

    @Override
    public <R> Collection<R> project(@Nonnull Projection<? super Map.Entry<K, V>, R> projection,
                                     @Nonnull Predicate<K, V> predicate) {
        return project(projection, predicate, Target.ALL_NODES);
    }

    /**
     * Execute the {@code project} operation only on the given {@code
     * partitions}.
     * <p>
     * Performance note: the query will use non-partitioned indexes. If not all
     * partitions of a member are in the set, the index will return entries for
     * all the partitions, but those will be subsequently eliminated. If all
     * (or most) partitions of a member are in the set, the performance hit is
     * small.
     * <p>
     * <b>Warning:</b> {@code partitions} is mutated during the call.
     */
    public <R> Collection<R> project(@Nonnull Projection<? super Map.Entry<K, V>, R> projection,
                                     @Nonnull Predicate<K, V> predicate, PartitionIdSet partitions) {
        return project(projection, predicate, Target.createPartitionTarget(partitions));
    }

    private <R> Collection<R> project(@Nonnull Projection<? super Map.Entry<K, V>, R> projection,
                                      @Nonnull Predicate<K, V> predicate, Target target) {
        checkNotNull(projection, NULL_PROJECTION_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        checkNotPagingPredicate(predicate, "project");

        // HazelcastInstanceAware handled by cloning
        projection = serializationService.toObject(serializationService.toData(projection));

        QueryResult result = executeQueryInternal(predicate, null, projection, IterationType.VALUE, target);
        return transformToSet(serializationService, result, predicate, IterationType.VALUE, false, false);
    }

    protected Object invoke(Operation operation, int partitionId) throws Throwable {
        Future future = operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
        Object response = future.get();
        Object result = toObject(response);
        if (result instanceof Throwable) {
            throw (Throwable) result;
        }
        return result;
    }

    /**
     * Returns an iterator for iterating entries in the {@code partitionId}. If
     * {@code prefetchValues} is {@code true}, values will be sent along with
     * the keys and no additional data will be fetched when iterating. If
     * {@code false}, only keys will be sent and values will be fetched when
     * calling {@code Map.Entry.getValue()} lazily.
     * <p>
     * The values are fetched in batches.
     * You may control the size of the batch by changing the {@code fetchSize}
     * parameter.
     * A too small {@code fetchSize} can affect performance since more data
     * will have to be sent to and from the partition owner.
     * A too high {@code fetchSize} means that more data will be sent which
     * can block other operations from being sent, including internal
     * operations.
     * The underlying implementation may send more values in one batch than
     * {@code fetchSize} if it needs to get to a "safepoint" to later resume
     * iteration.
     * <p>
     * <b>NOTE</b>
     * The iteration may be done when the map is being mutated or when there are
     * membership changes. The iterator does not reflect the state when it has
     * been constructed - it may return some entries that were added after the
     * iteration has started and may not return some entries that were removed
     * after iteration has started.
     * The iterator will not, however, skip an entry if it has not been changed
     * and will not return an entry twice.
     *
     * @param fetchSize      the size of the batches which will be sent when iterating the data
     * @param partitionId    the partition ID which is being iterated
     * @param prefetchValues whether to send values along with keys (if {@code true}) or
     *                       to fetch them lazily when iterating (if {@code false})
     * @return an iterator for the entries in the partition
     */
    @Nonnull
    public Iterator<Entry<K, V>> iterator(int fetchSize, int partitionId, boolean prefetchValues) {
        return new MapPartitionIterator<>(this, fetchSize, partitionId, prefetchValues);
    }

    /**
     * Returns an iterator for iterating the result of the projection on entries
     * in the {@code partitionId} which satisfy the {@code predicate}.
     * <p>
     * The values are fetched in batches.
     * You may control the size of the batch by changing the {@code fetchSize}
     * parameter.
     * A too small {@code fetchSize} can affect performance since more data
     * will have to be sent to and from the partition owner.
     * A too high {@code fetchSize} means that more data will be sent which
     * can block other operations from being sent, including internal
     * operations.
     * The underlying implementation may send more values in one batch than
     * {@code fetchSize} if it needs to get to a "safepoint" to later resume
     * iteration.
     * Predicates of type {@link PagingPredicate} are not supported.
     * <b>NOTE</b>
     * The iteration may be done when the map is being mutated or when there are
     * membership changes. The iterator does not reflect the state when it has
     * been constructed - it may return some entries that were added after the
     * iteration has started and may not return some entries that were removed
     * after iteration has started.
     * The iterator will not, however, skip an entry if it has not been changed
     * and will not return an entry twice.
     *
     * @param fetchSize   the size of the batches which will be sent when iterating the data
     * @param partitionId the partition ID which is being iterated
     * @param projection  the projection to apply before returning the value. {@code null} value
     *                    is not allowed
     * @param predicate   the predicate which the entries must match. {@code null} value is not
     *                    allowed
     * @param <R>         the return type
     * @return an iterator for the projected entries
     * @throws IllegalArgumentException if the predicate is of type {@link PagingPredicate}
     * @since 3.9
     */
    @Nonnull
    public <R> Iterator<R> iterator(
            int fetchSize,
            int partitionId,
            @Nonnull Projection<? super Map.Entry<K, V>, R> projection,
            @Nonnull Predicate<K, V> predicate
    ) {
        if (predicate instanceof PagingPredicate) {
            throw new IllegalArgumentException("Paging predicate is not allowed when iterating map by query");
        }
        checkNotNull(projection, NULL_PROJECTION_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        // HazelcastInstanceAware handled by cloning
        projection = serializationService.toObject(serializationService.toData(projection));
        handleHazelcastInstanceAwareParams(predicate);
        return new MapQueryPartitionIterator<>(this, fetchSize, partitionId, predicate, projection);
    }

    @Override
    @Nonnull
    public Iterator<Entry<K, V>> iterator() {
        int partitionCount = partitionService.getPartitionCount();
        return new MapIterator<>(this, partitionCount, false);
    }

    @Override
    @Nonnull
    public Iterator<Entry<K, V>> iterator(int fetchSize) {
        int partitionCount = partitionService.getPartitionCount();
        return new MapIterator<>(this, fetchSize, partitionCount, false);
    }

    /**
     * Returns an iterable providing an iterator for iterating the result
     * of the projection on entries in the {@code partitionId} which
     * satisfy the {@code predicate}.
     *
     * @param fetchSize   the size of the batches which will be sent when iterating the data
     * @param partitionId the partition ID which is being iterated
     * @param projection  the projection to apply before returning the value. null value is not allowed
     * @param predicate   the predicate which the entries must match. null value is not allowed
     * @param <R>         the return type
     * @return an iterable {@link MapQueryPartitionIterable} for the projected entries
     */
    @Nonnull
    public <R> Iterable<R> iterable(
            int fetchSize,
            int partitionId,
            @Nonnull Projection<? super Map.Entry<K, V>, R> projection,
            @Nonnull Predicate<K, V> predicate
    ) {
        checkNotNull(projection, NULL_PROJECTION_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        return new MapQueryPartitionIterable<>(this, fetchSize, partitionId, projection, predicate);
    }

    /**
     * Returns an iterable for iterating entries in the {@code partitionId}. If
     * {@code prefetchValues} is {@code true}, values will be sent along with
     * the keys and no additional data will be fetched when iterating. If
     * {@code false}, only keys will be sent and values will be fetched when
     * calling {@code Map.Entry.getValue()} lazily.
     *
     * @param fetchSize      the size of the batches which will be fetched when iterating the data
     * @param partitionId    the partition ID which is being iterated
     * @param prefetchValues whether to send values along with keys (if true) or to fetch them lazily when iterating (if false)
     * @return an iterable for the entries in the partition
     */
    @Nonnull
    public Iterable<Entry<K, V>> iterable(int fetchSize, int partitionId, boolean prefetchValues) {
        return new MapPartitionIterable<>(this, fetchSize, partitionId, prefetchValues);
    }

    /**
     * Returns an iterable for iterating the result of the projection on entries
     * in all of the partitions which satisfy the {@code predicate}.
     *
     * @param fetchSize  the size of the batches which will be fetched when iterating the data
     * @param projection the projection to apply before returning the value. null value is not allowed
     * @param predicate  the predicate which the entries must match. null value is not allowed
     * @param <R>        the return type
     * @return an iterable for the projected entries
     */
    @Nonnull
    public <R> Iterable<R> iterable(
            int fetchSize,
            @Nonnull Projection<? super Map.Entry<K, V>, R> projection,
            @Nonnull Predicate<K, V> predicate
    ) {
        checkNotNull(projection, NULL_PROJECTION_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        int partitionCount = partitionService.getPartitionCount();
        return new MapQueryIterable<>(this, fetchSize, partitionCount, projection, predicate);
    }

    /**
     * Returns an iterable for iterating entries in the all of the partitions. If
     * {@code prefetchValues} is {@code true}, values will be sent along with
     * the keys and no additional data will be fetched when iterating. If
     * {@code false}, only keys will be sent and values will be fetched when
     * calling {@code Map.Entry.getValue()} lazily.
     *
     * @param fetchSize      the size of the batches which will be sent when iterating the data
     * @param prefetchValues whether to send values along with keys (if true) or to fetch them lazily when iterating (if false)
     * @return an iterable for the map entries
     */
    @Nonnull
    public Iterable<Entry<K, V>> iterable(int fetchSize, boolean prefetchValues) {
        int partitionCount = partitionService.getPartitionCount();
        return new MapIterable<>(this, fetchSize, partitionCount, prefetchValues);
    }

    @Override
    public CompletionStage<EventJournalInitialSubscriberState> subscribeToEventJournal(int partitionId) {
        final MapEventJournalSubscribeOperation op = new MapEventJournalSubscribeOperation(name);
        op.setPartitionId(partitionId);
        return operationService.invokeOnPartition(op);
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation will skip cloning of the predicate and projection
     * for performance reasons. Because of this, the results of the projection
     * and predicate should not depend on any state that will be lost while
     * cloning. If you wish to get rid of user state, you may clone the predicate
     * and projection and keep them cached for all calls to this method to avoid
     * the overhead of cloning.
     */
    @Override
    public <T> CompletionStage<ReadResultSet<T>> readFromEventJournal(
            long startSequence,
            int minSize,
            int maxSize,
            int partitionId,
            java.util.function.Predicate<? super EventJournalMapEvent<K, V>> predicate,
            java.util.function.Function<? super EventJournalMapEvent<K, V>, ? extends T> projection) {
        if (maxSize < minSize) {
            throw new IllegalArgumentException("maxSize " + maxSize
                    + " must be greater or equal to minSize " + minSize);
        }
        final ManagedContext context = serializationService.getManagedContext();
        predicate = (java.util.function.Predicate<? super EventJournalMapEvent<K, V>>) context.initialize(predicate);
        projection = (java.util.function.Function<? super EventJournalMapEvent<K, V>, ? extends T>)
                context.initialize(projection);
        final MapEventJournalReadOperation<K, V, T> op = new MapEventJournalReadOperation<>(
                name, startSequence, minSize, maxSize, predicate, projection);
        op.setPartitionId(partitionId);
        return operationService.invokeOnPartition(op);
    }

    @Override
    public String toString() {
        return "IMap{name='" + name + '\'' + '}';
    }

    @Override
    public QueryCache<K, V> getQueryCache(@Nonnull String name) {
        checkNotNull(name, "name cannot be null");

        return getQueryCacheInternal(name, null, null, null, this);
    }

    @Override
    public QueryCache<K, V> getQueryCache(@Nonnull String name,
                                          @Nonnull Predicate<K, V> predicate,
                                          boolean includeValue) {
        checkNotNull(name, "name cannot be null");
        checkNotNull(predicate, "predicate cannot be null");
        checkNotInstanceOf(PagingPredicate.class, predicate, "predicate");
        handleHazelcastInstanceAwareParams(predicate);

        return getQueryCacheInternal(name, null, predicate, includeValue, this);
    }

    @Override
    public QueryCache<K, V> getQueryCache(@Nonnull String name,
                                          @Nonnull MapListener listener,
                                          @Nonnull Predicate<K, V> predicate,
                                          boolean includeValue) {
        checkNotNull(name, "name cannot be null");
        checkNotNull(listener, "listener cannot be null");
        checkNotNull(predicate, "predicate cannot be null");
        checkNotInstanceOf(PagingPredicate.class, predicate, "predicate");
        handleHazelcastInstanceAwareParams(listener, predicate);

        return getQueryCacheInternal(name, listener, predicate, includeValue, this);
    }

    private QueryCache<K, V> getQueryCacheInternal(@Nonnull String name,
                                                   @Nullable MapListener listener,
                                                   @Nullable Predicate<K, V> predicate,
                                                   @Nullable Boolean includeValue,
                                                   @Nonnull IMap<K, V> map) {
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();

        QueryCacheRequest request = newQueryCacheRequest()
                .forMap(map)
                .withCacheName(name)
                .withListener(listener)
                .withPredicate(predicate)
                .withIncludeValue(includeValue)
                .withContext(queryCacheContext);

        return createQueryCache(request);
    }

    private QueryCache<K, V> createQueryCache(QueryCacheRequest request) {
        QueryCacheContext queryCacheContext = request.getContext();
        SubscriberContext subscriberContext = queryCacheContext.getSubscriberContext();
        QueryCacheEndToEndProvider queryCacheEndToEndProvider = subscriberContext.getEndToEndQueryCacheProvider();
        return queryCacheEndToEndProvider.getOrCreateQueryCache(request.getMapName(), request.getCacheName(),
                subscriberContext.newEndToEndConstructor(request));
    }

    private static void checkNotPagingPredicate(Predicate predicate, String method) {
        if (predicate instanceof PagingPredicate) {
            throw new IllegalArgumentException("PagingPredicate not supported in " + method + " method");
        }
    }

    @Override
    public V computeIfPresent(@Nonnull K key,
                              @Nonnull BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_BIFUNCTION_IS_NOT_ALLOWED);

        if (SerializationUtil.isClassStaticAndSerializable(remappingFunction)
                && isClusterVersionGreaterOrEqual(Versions.V4_1)) {
            ComputeIfPresentEntryProcessor<K, V> ep = new ComputeIfPresentEntryProcessor<>(remappingFunction);
            return executeOnKey(key, ep);
        } else {
            return computeIfPresentLocally(key, remappingFunction);
        }
    }

    private V computeIfPresentLocally(K key,
                                      BiFunction<? super K, ? super V, ? extends V> remappingFunction) {

        while (true) {
            Data oldValueAsData = toData(getInternal(key));
            if (oldValueAsData == null) {
                return null;
            }

            V oldValueClone = toObject(oldValueAsData);
            V newValue = remappingFunction.apply(key, oldValueClone);
            if (newValue != null) {
                if (replaceInternal(key, oldValueAsData, toData(newValue))) {
                    return newValue;
                }
            } else if (removeInternal(key, oldValueAsData)) {
                return null;
            }
        }
    }

    @Override
    public V computeIfAbsent(@Nonnull K key, @Nonnull Function<? super K, ? extends V> mappingFunction) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(mappingFunction, NULL_FUNCTION_IS_NOT_ALLOWED);

        if (SerializationUtil.isClassStaticAndSerializable(mappingFunction)
                && isClusterVersionGreaterOrEqual(Versions.V4_1)) {
            ComputeIfAbsentEntryProcessor<K, V> ep = new ComputeIfAbsentEntryProcessor<>(mappingFunction);
            return executeOnKey(key, ep);
        } else {
            return computeIfAbsentLocally(key, mappingFunction);
        }
    }

    private V computeIfAbsentLocally(K key, Function<? super K, ? extends V> mappingFunction) {
        V oldValue = toObject(getInternal(key));
        if (oldValue != null) {
            return oldValue;
        }

        V newValue = mappingFunction.apply(key);
        if (newValue == null) {
            return null;
        }

        Data result = putIfAbsentInternal(key, toData(newValue), UNSET, TimeUnit.MILLISECONDS, UNSET, TimeUnit.MILLISECONDS);
        if (result == null) {
            return newValue;
        } else {
            return toObject(result);
        }
    }

    @Override
    public void forEach(@Nonnull BiConsumer<? super K, ? super V> action) {
        checkNotNull(action, NULL_CONSUMER_IS_NOT_ALLOWED);

        if (SerializationUtil.isClassStaticAndSerializable(action)
                && isClusterVersionGreaterOrEqual(Versions.V4_1)) {
            KeyValueConsumingEntryProcessor<K, V> ep = new KeyValueConsumingEntryProcessor<>(action);
            executeOnEntries(ep);
        } else {
            super.forEach(action);
        }
    }

    public V compute(@Nonnull K key, @Nonnull BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_BIFUNCTION_IS_NOT_ALLOWED);

        if (SerializationUtil.isClassStaticAndSerializable(remappingFunction)
                && isClusterVersionGreaterOrEqual(Versions.V4_1)) {
            ComputeEntryProcessor<K, V> ep = new ComputeEntryProcessor<>(remappingFunction);
            return executeOnKey(key, ep);
        } else {
            return computeLocally(key, remappingFunction);
        }
    }

    private V computeLocally(K key,
                             BiFunction<? super K, ? super V, ? extends V> remappingFunction) {

        while (true) {
            Data oldValueAsData = toData(getInternal(key));
            V oldValueClone = toObject(oldValueAsData);
            V newValue = remappingFunction.apply(key, oldValueClone);

            if (oldValueAsData != null) {
                if (newValue != null) {
                    if (replaceInternal(key, oldValueAsData, toData(newValue))) {
                        return newValue;
                    }
                } else if (removeInternal(key, oldValueAsData)) {
                    return null;
                }
            } else {
                if (newValue != null) {
                    Data result = putIfAbsentInternal(key, toData(newValue), UNSET, TimeUnit.MILLISECONDS, UNSET,
                            TimeUnit.MILLISECONDS);
                    if (result == null) {
                        return newValue;
                    }
                } else {
                    return null;
                }
            }
        }
    }

    public V merge(@Nonnull K key, @Nonnull V value,
                   @Nonnull BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(remappingFunction, NULL_BIFUNCTION_IS_NOT_ALLOWED);

        if (SerializationUtil.isClassStaticAndSerializable(remappingFunction)
                && isClusterVersionGreaterOrEqual(Versions.V4_1)) {
            MergeEntryProcessor<K, V> ep = new MergeEntryProcessor<>(remappingFunction, value);
            return executeOnKey(key, ep);
        } else {
            return mergeLocally(key, value, remappingFunction);
        }
    }

    private V mergeLocally(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        Data keyAsData = toDataWithStrategy(key);

        while (true) {
            Data oldValueAsData = toData(getInternal(keyAsData));
            if (oldValueAsData != null) {
                V oldValueClone = toObject(oldValueAsData);
                V newValue = remappingFunction.apply(oldValueClone, value);
                if (newValue != null) {
                    if (replaceInternal(keyAsData, oldValueAsData, toData(newValue))) {
                        return newValue;
                    }
                } else if (removeInternal(keyAsData, oldValueAsData)) {
                    return null;
                }
            } else {
                Data result = putIfAbsentInternal(keyAsData, toData(value), UNSET, TimeUnit.MILLISECONDS, UNSET,
                        TimeUnit.MILLISECONDS);
                if (result == null) {
                    return value;
                }
            }
        }
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        checkNotNull(function, NULL_BIFUNCTION_IS_NOT_ALLOWED);

        if (SerializationUtil.isClassStaticAndSerializable(function)
                && isClusterVersionGreaterOrEqual(Versions.V4_1)) {
            MapEntryReplacingEntryProcessor<K, V> ep = new MapEntryReplacingEntryProcessor<>(function);
            executeOnEntries(ep);
        } else {
            super.replaceAll(function);
        }
    }

}
