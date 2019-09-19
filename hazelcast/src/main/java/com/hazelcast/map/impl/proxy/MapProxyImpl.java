/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import com.hazelcast.internal.journal.EventJournalReader;
import com.hazelcast.internal.util.SimpleCompletableFuture;
import com.hazelcast.internal.util.SimpleCompletedFuture;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.iterator.MapPartitionIterator;
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
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.internal.util.executor.DelegatingFuture;

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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.query.QueryResultUtils.transformToSet;
import static com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequest.newQueryCacheRequest;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_MAX_IDLE;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_TTL;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.Preconditions.checkNoNullInside;
import static com.hazelcast.internal.util.Preconditions.checkNotInstanceOf;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.internal.util.SetUtil.createHashSet;
import static com.hazelcast.internal.util.TimeUtil.timeInMsOrTimeIfNullUnit;
import static java.util.Collections.emptyMap;

/**
 * Proxy implementation of {@link IMap} interface.
 *
 * @param <K> the key type of map.
 * @param <V> the value type of map.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
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
        return put(key, value, DEFAULT_TTL, TimeUnit.MILLISECONDS);
    }

    @Override
    public V put(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        Data result = putInternal(key, valueData, ttl, timeunit, DEFAULT_MAX_IDLE, TimeUnit.MILLISECONDS);
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
        return putIfAbsent(key, value, DEFAULT_TTL, TimeUnit.MILLISECONDS);
    }

    @Override
    public V putIfAbsent(@Nonnull K key, @Nonnull V value,
                         long ttl, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        Data result = putIfAbsentInternal(key, valueData, ttl, timeunit, DEFAULT_MAX_IDLE, TimeUnit.MILLISECONDS);
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
        putTransientInternal(key, valueData, ttl, timeunit, DEFAULT_MAX_IDLE, TimeUnit.MILLISECONDS);
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
        set(key, value, DEFAULT_TTL, TimeUnit.MILLISECONDS);
    }

    @Override
    public void set(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit ttlUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(ttlUnit, NULL_TTL_UNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        setInternal(key, valueData, ttl, ttlUnit, DEFAULT_MAX_IDLE, TimeUnit.MILLISECONDS);
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
        checkPositive(leaseTime, "leaseTime should be positive");

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
    public ICompletableFuture<V> getAsync(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return new DelegatingFuture<>(getAsyncInternal(key), serializationService);
    }

    @Override
    public boolean isLocked(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toDataWithStrategy(key);
        return lockSupport.isLocked(getNodeEngine(), keyData);
    }

    @Override
    public ICompletableFuture<V> putAsync(@Nonnull K key, @Nonnull V value) {
        return putAsync(key, value, DEFAULT_TTL, TimeUnit.MILLISECONDS);
    }

    @Override
    public ICompletableFuture<V> putAsync(@Nonnull K key, @Nonnull V value,
                                          long ttl, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return new DelegatingFuture<>(
                putAsyncInternal(key, valueData, ttl, timeunit, DEFAULT_MAX_IDLE, TimeUnit.MILLISECONDS),
                serializationService);
    }

    @Override
    public ICompletableFuture<V> putAsync(@Nonnull K key, @Nonnull V value,
                                          long ttl, @Nonnull TimeUnit ttlUnit,
                                          long maxIdle, @Nonnull TimeUnit maxIdleUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(ttlUnit, NULL_TTL_UNIT_IS_NOT_ALLOWED);
        checkNotNull(maxIdleUnit, NULL_MAX_IDLE_UNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return new DelegatingFuture<>(
                putAsyncInternal(key, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit),
                serializationService);
    }

    @Override
    public ICompletableFuture<Void> setAsync(@Nonnull K key, @Nonnull V value) {
        return setAsync(key, value, DEFAULT_TTL, TimeUnit.MILLISECONDS);
    }

    @Override
    public ICompletableFuture<Void> setAsync(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeunit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return new DelegatingFuture<>(
                setAsyncInternal(key, valueData, ttl, timeunit, DEFAULT_MAX_IDLE, TimeUnit.MILLISECONDS),
                serializationService);
    }

    @Override
    public ICompletableFuture<Void> setAsync(@Nonnull K key,
                                             @Nonnull V value,
                                             long ttl, @Nonnull TimeUnit ttlUnit,
                                             long maxIdle, @Nonnull TimeUnit maxIdleUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(ttlUnit, NULL_TTL_UNIT_IS_NOT_ALLOWED);
        checkNotNull(maxIdleUnit, NULL_MAX_IDLE_UNIT_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return new DelegatingFuture<>(
                setAsyncInternal(key, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit),
                serializationService);
    }

    @Override
    public ICompletableFuture<V> removeAsync(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return new DelegatingFuture<>(removeAsyncInternal(key), serializationService);
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
        putAllInternal(map, null);
    }

    /**
     * This version does not support batching. Don't mutate the given map until the
     * future completes.
     */
    // used by jet
    public ICompletableFuture<Void> putAllAsync(@Nonnull Map<? extends K, ? extends V> map) {
        checkNotNull(map, "Null argument map is not allowed");
        SimpleCompletableFuture<Void> future = new SimpleCompletableFuture<>(getNodeEngine());
        putAllInternal(map, future);
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
    public String addLocalEntryListener(@Nonnull MapListener listener) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addLocalEntryListenerInternal(listener);
    }

    @Override
    public String addLocalEntryListener(@Nonnull EntryListener<K, V> listener) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addLocalEntryListenerInternal(listener);
    }

    @Override
    public String addLocalEntryListener(@Nonnull MapListener listener,
                                        @Nonnull Predicate<K, V> predicate,
                                        boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addLocalEntryListenerInternal(listener, predicate, null, includeValue);
    }

    @Override
    public String addLocalEntryListener(@Nonnull EntryListener<K, V> listener,
                                        @Nonnull Predicate<K, V> predicate,
                                        boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addLocalEntryListenerInternal(listener, predicate, null, includeValue);
    }

    @Override
    public String addLocalEntryListener(@Nonnull MapListener listener,
                                        @Nonnull Predicate<K, V> predicate,
                                        @Nullable K key,
                                        boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addLocalEntryListenerInternal(listener, predicate, toDataWithStrategy(key), includeValue);
    }

    @Override
    public String addLocalEntryListener(@Nonnull EntryListener<K, V> listener,
                                        @Nonnull Predicate<K, V> predicate,
                                        @Nullable K key,
                                        boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addLocalEntryListenerInternal(listener, predicate, toDataWithStrategy(key), includeValue);
    }

    @Override
    public String addEntryListener(@Nonnull MapListener listener, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addEntryListenerInternal(listener, null, includeValue);
    }

    @Override
    public String addEntryListener(@Nonnull EntryListener<K, V> listener, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addEntryListenerInternal(listener, null, includeValue);
    }

    @Override
    public String addEntryListener(@Nonnull MapListener listener, @Nonnull K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addEntryListenerInternal(listener, toDataWithStrategy(key), includeValue);
    }

    @Override
    public String addEntryListener(@Nonnull EntryListener<K, V> listener, @Nonnull K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addEntryListenerInternal(listener, toDataWithStrategy(key), includeValue);
    }

    @Override
    public String addEntryListener(@Nonnull MapListener listener,
                                   @Nonnull Predicate<K, V> predicate,
                                   @Nullable K key,
                                   boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addEntryListenerInternal(listener, predicate, toDataWithStrategy(key), includeValue);
    }

    @Override
    public String addEntryListener(@Nonnull EntryListener<K, V> listener,
                                   @Nonnull Predicate<K, V> predicate,
                                   @Nullable K key,
                                   boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addEntryListenerInternal(listener, predicate, toDataWithStrategy(key), includeValue);
    }

    @Override
    public String addEntryListener(@Nonnull MapListener listener,
                                   @Nonnull Predicate<K, V> predicate,
                                   boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addEntryListenerInternal(listener, predicate, null, includeValue);
    }

    @Override
    public String addEntryListener(@Nonnull EntryListener<K, V> listener,
                                   @Nonnull Predicate<K, V> predicate,
                                   boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addEntryListenerInternal(listener, predicate, null, includeValue);
    }

    @Override
    public boolean removeEntryListener(@Nonnull String id) {
        checkNotNull(id, "Listener ID should not be null!");

        return removeEntryListenerInternal(id);
    }

    @Override
    public String addPartitionLostListener(@Nonnull MapPartitionLostListener listener) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addPartitionLostListenerInternal(listener);
    }

    @Override
    public boolean removePartitionLostListener(@Nonnull String id) {
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
        return executePredicate(predicate, IterationType.KEY, true);
    }

    @Nonnull
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return entrySet(Predicates.alwaysTrue());
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet(@Nonnull Predicate predicate) {
        return executePredicate(predicate, IterationType.ENTRY, true);
    }

    @Nonnull
    @Override
    public Collection<V> values() {
        return values(Predicates.alwaysTrue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<V> values(@Nonnull Predicate predicate) {
        return executePredicate(predicate, IterationType.VALUE, false);
    }

    private Set executePredicate(Predicate predicate, IterationType iterationType, boolean uniqueResult) {
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        QueryResult result = executeQueryInternal(predicate, iterationType, Target.ALL_NODES);
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

        Data result = executeOnKeyInternal(key, entryProcessor);
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

    /**
     * Async version of {@link #executeOnKeys}.
     */
    public <R> ICompletableFuture<Map<K, R>> submitToKeys(@Nonnull Set<K> keys,
                                                          @Nonnull EntryProcessor<K, V, R> entryProcessor) {
        checkNotNull(keys, NULL_KEYS_ARE_NOT_ALLOWED);
        if (keys.isEmpty()) {
            return new SimpleCompletedFuture<>(Collections.emptyMap());
        }
        handleHazelcastInstanceAwareParams(entryProcessor);

        Set<Data> dataKeys = createHashSet(keys.size());
        return submitToKeysInternal(keys, dataKeys, entryProcessor);
    }

    @Override
    public <R> void submitToKey(@Nonnull K key,
                                @Nonnull EntryProcessor<K, V, R> entryProcessor,
                                ExecutionCallback<? super R> callback) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(entryProcessor, callback);

        executeOnKeyInternal(key, entryProcessor, callback);
    }

    @Override
    public <R> ICompletableFuture<R> submitToKey(@Nonnull K key,
                                                 @Nonnull EntryProcessor<K, V, R> entryProcessor) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(entryProcessor);

        InternalCompletableFuture future = executeOnKeyInternal(key, entryProcessor, null);
        return new DelegatingFuture(future, serializationService);
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
        checkNotNull(projection, NULL_PROJECTION_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        checkNotPagingPredicate(predicate, "project");

        // HazelcastInstanceAware handled by cloning
        projection = serializationService.toObject(serializationService.toData(projection));

        QueryResult result = executeQueryInternal(predicate, null, projection, IterationType.VALUE, Target.ALL_NODES);
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
     * Returns an iterator for iterating entries in the {@code partitionId}. If {@code prefetchValues} is
     * {@code true}, values will be sent along with the keys and no additional data will be fetched when
     * iterating. If {@code false}, only keys will be sent and values will be fetched when calling {@code
     * Map.Entry.getValue()} lazily.
     * <p>
     * The entries are not fetched one-by-one but in batches.
     * You may control the size of the batch by changing the {@code fetchSize} parameter.
     * A too small {@code fetchSize} can affect performance since more data will have to be sent to and from the partition owner.
     * A too high {@code fetchSize} means that more data will be sent which can block other operations from being sent,
     * including internal operations.
     * The underlying implementation may send more values in one batch than {@code fetchSize} if it needs to get to
     * a "safepoint" to resume iteration later.
     * <p>
     * <b>NOTE</b>
     * Iterating the map should be done only when the {@link IMap} is not being
     * mutated and the cluster is stable (there are no migrations or membership changes).
     * In other cases, the iterator may not return some entries or may return an entry twice.
     *
     * @param fetchSize      the size of the batches which will be sent when iterating the data
     * @param partitionId    the partition ID which is being iterated
     * @param prefetchValues whether to send values along with keys (if {@code true}) or
     *                       to fetch them lazily when iterating (if {@code false})
     * @return the iterator for the projected entries
     */
    public Iterator<Entry<K, V>> iterator(int fetchSize, int partitionId, boolean prefetchValues) {
        return new MapPartitionIterator<>(this, fetchSize, partitionId, prefetchValues);
    }

    /**
     * Returns an iterator for iterating the result of the projection on entries in the {@code partitionId} which
     * satisfy the {@code predicate}.
     * <p>
     * The values are not fetched one-by-one but rather in batches.
     * You may control the size of the batch by changing the {@code fetchSize} parameter.
     * A too small {@code fetchSize} can affect performance since more data will have to be sent to and from the partition owner.
     * A too high {@code fetchSize} means that more data will be sent which can block other operations from being sent,
     * including internal operations.
     * The underlying implementation may send more values in one batch than {@code fetchSize} if it needs to get to
     * a "safepoint" to later resume iteration.
     * Predicates of type {@link PagingPredicate} are not supported.
     * <p>
     * <b>NOTE</b>
     * Iterating the map should be done only when the {@link IMap} is not being
     * mutated and the cluster is stable (there are no migrations or membership changes).
     * In other cases, the iterator may not return some entries or may return an entry twice.
     *
     * @param fetchSize   the size of the batches which will be sent when iterating the data
     * @param partitionId the partition ID which is being iterated
     * @param projection  the projection to apply before returning the value. {@code null} value is not allowed
     * @param predicate   the predicate which the entries must match. {@code null} value is not allowed
     * @param <R>         the return type
     * @return the iterator for the projected entries
     * @throws IllegalArgumentException if the predicate is of type {@link PagingPredicate}
     * @since 3.9
     */
    public <R> Iterator<R> iterator(int fetchSize, int partitionId,
                                    Projection<? super Map.Entry<K, V>, R> projection,
                                    Predicate<K, V> predicate) {
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
    public ICompletableFuture<EventJournalInitialSubscriberState> subscribeToEventJournal(int partitionId) {
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
    public <T> ICompletableFuture<ReadResultSet<T>> readFromEventJournal(
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
        context.initialize(predicate);
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
}
