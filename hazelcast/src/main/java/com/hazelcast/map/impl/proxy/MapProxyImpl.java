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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.journal.EventJournalInitialSubscriberState;
import com.hazelcast.internal.journal.EventJournalReader;
import com.hazelcast.internal.util.SimpleCompletedFuture;
import com.hazelcast.map.EntryProcessor;
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
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.MappingJob;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.ReducingSubmittableJob;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.CollectionUtil;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.executor.DelegatingFuture;

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

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.internal.cluster.Versions.V3_11;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.query.QueryResultUtils.transformToSet;
import static com.hazelcast.map.impl.querycache.subscriber.QueryCacheRequest.newQueryCacheRequest;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_MAX_IDLE;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.MapUtil.createHashMap;
import static com.hazelcast.util.Preconditions.checkNoNullInside;
import static com.hazelcast.util.Preconditions.checkNotInstanceOf;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.Preconditions.checkTrue;
import static com.hazelcast.util.Preconditions.isNotNull;
import static com.hazelcast.util.SetUtil.createHashSet;
import static java.util.Collections.emptyMap;

/**
 * Proxy implementation of {@link com.hazelcast.core.IMap} interface.
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
    public V get(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return toObject(getInternal(key));
    }

    @Override
    public V put(K key, V value) {
        return put(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        Data result = putInternal(key, valueData, ttl, timeunit, DEFAULT_MAX_IDLE, TimeUnit.MILLISECONDS);
        return toObject(result);
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
        if (isClusterVersionLessThan(V3_11)) {
            throw new UnsupportedOperationException("put with Max-Idle operation is available "
                    + "when cluster version is 3.11 or higher");
        }

        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        Data result = putInternal(key, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit);
        return toObject(result);
    }

    @Override
    public boolean tryPut(K key, V value, long timeout, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return tryPutInternal(key, valueData, timeout, timeunit);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return putIfAbsent(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public V putIfAbsent(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        Data result = putIfAbsentInternal(key, valueData, ttl, timeunit, DEFAULT_MAX_IDLE, TimeUnit.MILLISECONDS);
        return toObject(result);
    }

    @Override
    public V putIfAbsent(K key, V value, long ttl, TimeUnit timeunit, long maxIdle, TimeUnit maxIdleUnit) {
        if (isClusterVersionLessThan(V3_11)) {
            throw new UnsupportedOperationException("putIfAbsent with Max-Idle operation is available "
                    + "when cluster version is 3.11 or higher");
        }

        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        Data result = putIfAbsentInternal(key, valueData, ttl, timeunit, maxIdle, maxIdleUnit);
        return toObject(result);
    }

    @Override
    public void putTransient(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        putTransientInternal(key, valueData, ttl, timeunit, DEFAULT_MAX_IDLE, TimeUnit.MILLISECONDS);
    }

    @Override
    public void putTransient(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
        if (isClusterVersionLessThan(V3_11)) {
            throw new UnsupportedOperationException("putTransient with Max-Idle operation is available "
                    + "when cluster version is 3.11 or higher");
        }

        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        putTransientInternal(key, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(oldValue, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(newValue, NULL_VALUE_IS_NOT_ALLOWED);

        Data oldValueData = toData(oldValue);
        Data newValueData = toData(newValue);
        return replaceInternal(key, oldValueData, newValueData);
    }

    @Override
    public V replace(K key, V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return toObject(replaceInternal(key, valueData));
    }

    @Override
    public void set(K key, V value) {
        set(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public void set(K key, V value, long ttl, TimeUnit ttlUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        setInternal(key, valueData, ttl, ttlUnit, DEFAULT_MAX_IDLE, TimeUnit.MILLISECONDS);
    }

    @Override
    public void set(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
        if (isClusterVersionLessThan(V3_11)) {
            throw new UnsupportedOperationException("set with Max-Idle operation is available "
                    + "when cluster version is 3.11 or higher");
        }

        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        setInternal(key, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit);
    }

    @Override
    public V remove(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data result = removeInternal(key);
        return toObject(result);
    }

    @Override
    public boolean remove(Object key, Object value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return removeInternal(key, valueData);
    }

    @Override
    public void removeAll(Predicate<K, V> predicate) {
        checkNotNull(predicate, "predicate cannot be null");
        handleHazelcastInstanceAwareParams(predicate);

        removeAllInternal(predicate);
    }

    @Override
    public void delete(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        deleteInternal(key);
    }

    @Override
    public boolean containsKey(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return containsKeyInternal(key);
    }

    @Override
    public boolean containsValue(Object value) {
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return containsValueInternal(valueData);
    }

    @Override
    public void lock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toDataWithStrategy(key);
        lockSupport.lock(getNodeEngine(), keyData);
    }

    @Override
    public void lock(Object key, long leaseTime, TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkPositive(leaseTime, "leaseTime should be positive");

        Data keyData = toDataWithStrategy(key);
        lockSupport.lock(getNodeEngine(), keyData, timeUnit.toMillis(leaseTime));
    }

    @Override
    public void unlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toDataWithStrategy(key);
        lockSupport.unlock(getNodeEngine(), keyData);
    }

    @Override
    public boolean tryRemove(K key, long timeout, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return tryRemoveInternal(key, timeout, timeunit);
    }

    @Override
    public ICompletableFuture<V> getAsync(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return new DelegatingFuture<V>(getAsyncInternal(key), serializationService);
    }

    @Override
    public boolean isLocked(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toDataWithStrategy(key);
        return lockSupport.isLocked(getNodeEngine(), keyData);
    }

    @Override
    public ICompletableFuture<V> putAsync(K key, V value) {
        return putAsync(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public ICompletableFuture<V> putAsync(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return new DelegatingFuture<V>(
                putAsyncInternal(key, valueData, ttl, timeunit, DEFAULT_MAX_IDLE, TimeUnit.MILLISECONDS),
                serializationService);
    }

    @Override
    public ICompletableFuture<V> putAsync(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
        if (isClusterVersionLessThan(V3_11)) {
            throw new UnsupportedOperationException("putAsync with Max-Idle operation is available "
                    + "when cluster version is 3.11 or higher");
        }

        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return new DelegatingFuture<V>(
                putAsyncInternal(key, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit),
                serializationService);
    }

    @Override
    public ICompletableFuture<Void> setAsync(K key, V value) {
        return setAsync(key, value, -1, TimeUnit.MILLISECONDS);
    }

    @Override
    public ICompletableFuture<Void> setAsync(K key, V value, long ttl, TimeUnit timeunit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return new DelegatingFuture<Void>(
                setAsyncInternal(key, valueData, ttl, timeunit, DEFAULT_MAX_IDLE, TimeUnit.MILLISECONDS),
                serializationService);
    }

    @Override
    public ICompletableFuture<Void> setAsync(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdle, TimeUnit maxIdleUnit) {
        if (isClusterVersionLessThan(V3_11)) {
            throw new UnsupportedOperationException("setAsync with Max-Idle operation is available "
                    + "when cluster version is 3.11 or higher");
        }

        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        return new DelegatingFuture<Void>(
                setAsyncInternal(key, valueData, ttl, ttlUnit, maxIdle, maxIdleUnit),
                serializationService);
    }

    @Override
    public ICompletableFuture<V> removeAsync(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        return new DelegatingFuture<V>(removeAsyncInternal(key), serializationService);
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        if (CollectionUtil.isEmpty(keys)) {
            return emptyMap();
        }

        int keysSize = keys.size();
        List<Data> dataKeys = new LinkedList<Data>();
        List<Object> resultingKeyValuePairs = new ArrayList<Object>(keysSize * 2);
        getAllInternal(keys, dataKeys, resultingKeyValuePairs);

        Map<K, V> result = createHashMap(keysSize);
        for (int i = 0; i < resultingKeyValuePairs.size(); ) {
            K key = toObject(resultingKeyValuePairs.get(i++));
            V value = toObject(resultingKeyValuePairs.get(i++));
            result.put(key, value);
        }
        return result;
    }

    @Override
    public boolean setTtl(K key, long ttl, TimeUnit timeunit) {
        checkNotNull(key);
        checkNotNull(timeunit);
        return setTtlInternal(key, ttl, timeunit);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        checkNotNull(map, "Null argument map is not allowed");
        putAllInternal(map);
    }

    @Override
    public boolean tryLock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toDataWithStrategy(key);
        return lockSupport.tryLock(getNodeEngine(), keyData);
    }

    @Override
    public boolean tryLock(K key, long time, TimeUnit timeunit) throws InterruptedException {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toDataWithStrategy(key);
        return lockSupport.tryLock(getNodeEngine(), keyData, time, timeunit);
    }

    @Override
    public boolean tryLock(K key, long time, TimeUnit timeunit, long leaseTime, TimeUnit leaseTimeUnit)
            throws InterruptedException {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toDataWithStrategy(key);
        return lockSupport.tryLock(getNodeEngine(), keyData, time, timeunit, leaseTime, leaseTimeUnit);
    }

    @Override
    public void forceUnlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toDataWithStrategy(key);
        lockSupport.forceUnlock(getNodeEngine(), keyData);
    }

    @Override
    public String addInterceptor(MapInterceptor interceptor) {
        checkNotNull(interceptor, "Interceptor should not be null!");
        handleHazelcastInstanceAwareParams(interceptor);

        return addMapInterceptorInternal(interceptor);
    }

    @Override
    public void removeInterceptor(String id) {
        checkNotNull(id, "Interceptor ID should not be null!");

        removeMapInterceptorInternal(id);
    }

    @Override
    public String addLocalEntryListener(MapListener listener) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addLocalEntryListenerInternal(listener);
    }

    @Override
    public String addLocalEntryListener(EntryListener listener) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addLocalEntryListenerInternal(listener);
    }

    @Override
    public String addLocalEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addLocalEntryListenerInternal(listener, predicate, null, includeValue);
    }

    @Override
    public String addLocalEntryListener(EntryListener listener, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addLocalEntryListenerInternal(listener, predicate, null, includeValue);
    }

    @Override
    public String addLocalEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addLocalEntryListenerInternal(listener, predicate, toDataWithStrategy(key), includeValue);
    }

    @Override
    public String addLocalEntryListener(EntryListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addLocalEntryListenerInternal(listener, predicate, toDataWithStrategy(key), includeValue);
    }

    @Override
    public String addEntryListener(MapListener listener, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addEntryListenerInternal(listener, null, includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addEntryListenerInternal(listener, null, includeValue);
    }

    @Override
    public String addEntryListener(MapListener listener, K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addEntryListenerInternal(listener, toDataWithStrategy(key), includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addEntryListenerInternal(listener, toDataWithStrategy(key), includeValue);
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addEntryListenerInternal(listener, predicate, toDataWithStrategy(key), includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate<K, V> predicate, K key, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addEntryListenerInternal(listener, predicate, toDataWithStrategy(key), includeValue);
    }

    @Override
    public String addEntryListener(MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addEntryListenerInternal(listener, predicate, null, includeValue);
    }

    @Override
    public String addEntryListener(EntryListener listener, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener, predicate);

        return addEntryListenerInternal(listener, predicate, null, includeValue);
    }

    @Override
    public boolean removeEntryListener(String id) {
        checkNotNull(id, "Listener ID should not be null!");

        return removeEntryListenerInternal(id);
    }

    @Override
    public String addPartitionLostListener(MapPartitionLostListener listener) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(listener);

        return addPartitionLostListenerInternal(listener);
    }

    @Override
    public boolean removePartitionLostListener(String id) {
        checkNotNull(id, "Listener ID should not be null!");

        return removePartitionLostListenerInternal(id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public EntryView<K, V> getEntryView(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        SimpleEntryView<K, V> entryViewInternal = (SimpleEntryView<K, V>) getEntryViewInternal(toDataWithStrategy(key));
        if (entryViewInternal == null) {
            return null;
        }
        Data value = (Data) entryViewInternal.getValue();
        entryViewInternal.setKey(key);
        entryViewInternal.setValue((V) toObject(value));
        return entryViewInternal;
    }

    @Override
    public boolean evict(Object key) {
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
    public void loadAll(Set<K> keys, boolean replaceExistingValues) {
        checkTrue(isMapStoreEnabled(), "First you should configure a map store");
        checkNotNull(keys, NULL_KEYS_ARE_NOT_ALLOWED);
        checkNoNullInside(keys, NULL_KEY_IS_NOT_ALLOWED);

        loadInternal(keys, null, replaceExistingValues);
    }

    @Override
    public void clear() {
        clearInternal();
    }

    @Override
    public Set<K> keySet() {
        return keySet(TruePredicate.INSTANCE);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet(Predicate predicate) {
        return executePredicate(predicate, IterationType.KEY, true);
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return entrySet(TruePredicate.INSTANCE);
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet(Predicate predicate) {
        return executePredicate(predicate, IterationType.ENTRY, true);
    }

    @Override
    public Collection<V> values() {
        return values(TruePredicate.INSTANCE);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<V> values(Predicate predicate) {
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
        return localKeySet(TruePredicate.INSTANCE);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> localKeySet(Predicate predicate) {
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        QueryResult result = executeQueryInternal(predicate, IterationType.KEY, Target.LOCAL_NODE);
        incrementOtherOperationsStat();
        return transformToSet(serializationService, result, predicate, IterationType.KEY, false, false);
    }

    @Override
    public Object executeOnKey(K key, EntryProcessor entryProcessor) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(entryProcessor);

        Data result = executeOnKeyInternal(key, entryProcessor);
        return toObject(result);
    }

    @Override
    public Map<K, Object> executeOnKeys(Set<K> keys, EntryProcessor entryProcessor) {
        try {
            return submitToKeys(keys, entryProcessor).get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    /**
     * Async version of {@link #executeOnKeys}.
     */
    public ICompletableFuture<Map<K, Object>> submitToKeys(Set<K> keys, EntryProcessor entryProcessor) {
        checkNotNull(keys, NULL_KEYS_ARE_NOT_ALLOWED);
        if (keys.isEmpty()) {
            return new SimpleCompletedFuture<Map<K, Object>>(Collections.<K, Object>emptyMap());
        }
        handleHazelcastInstanceAwareParams(entryProcessor);

        Set<Data> dataKeys = createHashSet(keys.size());
        return submitToKeysInternal(keys, dataKeys, entryProcessor);
    }

    @Override
    public void submitToKey(K key, EntryProcessor entryProcessor, ExecutionCallback callback) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(entryProcessor, callback);

        executeOnKeyInternal(key, entryProcessor, callback);
    }

    @Override
    public ICompletableFuture submitToKey(K key, EntryProcessor entryProcessor) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        handleHazelcastInstanceAwareParams(entryProcessor);

        InternalCompletableFuture future = executeOnKeyInternal(key, entryProcessor, null);
        return new DelegatingFuture(future, serializationService);
    }

    @Override
    public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor) {
        return executeOnEntries(entryProcessor, TruePredicate.INSTANCE);
    }

    @Override
    public Map<K, Object> executeOnEntries(EntryProcessor entryProcessor, Predicate predicate) {
        handleHazelcastInstanceAwareParams(entryProcessor, predicate);
        List<Data> result = new ArrayList<Data>();

        executeOnEntriesInternal(entryProcessor, predicate, result);
        if (result.isEmpty()) {
            return emptyMap();
        }

        Map<K, Object> resultingMap = createHashMap(result.size() / 2);
        for (int i = 0; i < result.size(); ) {
            Data key = result.get(i++);
            Data value = result.get(i++);

            resultingMap.put((K) toObject(key), toObject(value));

        }
        return resultingMap;
    }

    @Override
    public <R> R aggregate(Aggregator<Map.Entry<K, V>, R> aggregator) {
        return aggregate(aggregator, TruePredicate.<K, V>truePredicate());
    }

    @Override
    public <R> R aggregate(Aggregator<Map.Entry<K, V>, R> aggregator, Predicate<K, V> predicate) {
        checkNotNull(aggregator, NULL_AGGREGATOR_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        checkNotPagingPredicate(predicate, "aggregate");

        // HazelcastInstanceAware handled by cloning
        aggregator = serializationService.toObject(serializationService.toData(aggregator));

        AggregationResult result = executeQueryInternal(predicate, aggregator, null, IterationType.ENTRY, Target.ALL_NODES);
        return result.<R>getAggregator().aggregate();
    }

    @Override
    public <R> Collection<R> project(Projection<Map.Entry<K, V>, R> projection) {
        return project(projection, TruePredicate.INSTANCE);
    }

    @Override
    public <R> Collection<R> project(Projection<Map.Entry<K, V>, R> projection, Predicate<K, V> predicate) {
        checkNotNull(projection, NULL_PROJECTION_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        checkNotPagingPredicate(predicate, "project");

        // HazelcastInstanceAware handled by cloning
        projection = serializationService.toObject(serializationService.toData(projection));

        QueryResult result = executeQueryInternal(predicate, null, projection, IterationType.VALUE, Target.ALL_NODES);
        return transformToSet(serializationService, result, predicate, IterationType.VALUE, false, false);
    }

    @Override
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation) {
        checkTrue(NATIVE != mapConfig.getInMemoryFormat(), "NATIVE storage format is not supported for MapReduce");

        HazelcastInstance hazelcastInstance = getNodeEngine().getHazelcastInstance();
        JobTracker jobTracker = hazelcastInstance.getJobTracker("hz::aggregation-map-" + getName());
        return aggregate(supplier, aggregation, jobTracker);
    }

    @Override
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation,
                                                    JobTracker jobTracker) {
        checkTrue(NATIVE != mapConfig.getInMemoryFormat(), "NATIVE storage format is not supported for MapReduce");

        try {
            isNotNull(jobTracker, "jobTracker");
            KeyValueSource<K, V> keyValueSource = KeyValueSource.fromMap(this);
            Job<K, V> job = jobTracker.newJob(keyValueSource);
            Mapper mapper = aggregation.getMapper(supplier);
            CombinerFactory combinerFactory = aggregation.getCombinerFactory();
            ReducerFactory reducerFactory = aggregation.getReducerFactory();
            Collator collator = aggregation.getCollator();

            MappingJob mappingJob = job.mapper(mapper);
            ReducingSubmittableJob reducingJob;
            if (combinerFactory == null) {
                reducingJob = mappingJob.reducer(reducerFactory);
            } else {
                reducingJob = mappingJob.combiner(combinerFactory).reducer(reducerFactory);
            }

            ICompletableFuture<Result> future = reducingJob.submit(collator);
            return future.get();
        } catch (Exception e) {
            // TODO: not what we want, because it can lead to wrapping of HazelcastException
            throw new HazelcastException(e);
        }
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
        return new MapPartitionIterator<K, V>(this, fetchSize, partitionId, prefetchValues);
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
    public <R> Iterator<R> iterator(int fetchSize, int partitionId, Projection<Map.Entry<K, V>, R> projection,
                                    Predicate<K, V> predicate) {
        if (predicate instanceof PagingPredicate) {
            throw new IllegalArgumentException("Paging predicate is not allowed when iterating map by query");
        }
        checkNotNull(projection, NULL_PROJECTION_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);
        // HazelcastInstanceAware handled by cloning
        projection = serializationService.toObject(serializationService.toData(projection));
        handleHazelcastInstanceAwareParams(predicate);
        return new MapQueryPartitionIterator<K, V, R>(this, fetchSize, partitionId, predicate, projection);
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
            com.hazelcast.util.function.Predicate<? super EventJournalMapEvent<K, V>> predicate,
            com.hazelcast.util.function.Function<? super EventJournalMapEvent<K, V>, ? extends T> projection) {
        if (maxSize < minSize) {
            throw new IllegalArgumentException("maxSize " + maxSize
                    + " must be greater or equal to minSize " + minSize);
        }
        final ManagedContext context = serializationService.getManagedContext();
        context.initialize(predicate);
        context.initialize(projection);
        final MapEventJournalReadOperation<K, V, T> op = new MapEventJournalReadOperation<K, V, T>(
                name, startSequence, minSize, maxSize, predicate, projection);
        op.setPartitionId(partitionId);
        return operationService.invokeOnPartition(op);
    }

    @Override
    public String toString() {
        return "IMap{name='" + name + '\'' + '}';
    }

    @Override
    public QueryCache<K, V> getQueryCache(String name) {
        checkNotNull(name, "name cannot be null");

        return getQueryCacheInternal(name, null, null, null, this);
    }

    @Override
    public QueryCache<K, V> getQueryCache(String name, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(name, "name cannot be null");
        checkNotNull(predicate, "predicate cannot be null");
        checkNotInstanceOf(PagingPredicate.class, predicate, "predicate");
        handleHazelcastInstanceAwareParams(predicate);

        return getQueryCacheInternal(name, null, predicate, includeValue, this);
    }

    @Override
    public QueryCache<K, V> getQueryCache(String name, MapListener listener, Predicate<K, V> predicate, boolean includeValue) {
        checkNotNull(name, "name cannot be null");
        checkNotNull(predicate, "predicate cannot be null");
        checkNotInstanceOf(PagingPredicate.class, predicate, "predicate");
        handleHazelcastInstanceAwareParams(listener, predicate);

        return getQueryCacheInternal(name, listener, predicate, includeValue, this);
    }

    private QueryCache<K, V> getQueryCacheInternal(String name, MapListener listener, Predicate<K, V> predicate,
                                                   Boolean includeValue, IMap<K, V> map) {
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
