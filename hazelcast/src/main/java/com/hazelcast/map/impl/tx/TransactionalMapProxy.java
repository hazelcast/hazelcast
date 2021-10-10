/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.tx;

import com.hazelcast.internal.nearcache.impl.NearCachingHook;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.map.impl.query.QueryEngine;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultUtils;
import com.hazelcast.map.impl.query.Target;
import com.hazelcast.map.impl.tx.TxnValueWrapper.Type;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.transaction.impl.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.Preconditions.checkNotInstanceOf;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.map.impl.record.Record.UNSET;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Proxy implementation of {@link TransactionalMap} interface.
 */
public class TransactionalMapProxy<K,V> extends TransactionalMapProxySupport implements TransactionalMap<K,V> {

    private final Map<Data, TxnValueWrapper> txMap = new HashMap<>();

    public TransactionalMapProxy(String name, MapService mapService, NodeEngine nodeEngine, Transaction transaction) {
        super(name, mapService, nodeEngine, transaction);
    }

    @Override
    public boolean containsKey(Object key) {
        return containsKey(key, false);
    }

    public boolean containsKey(Object key, boolean skipNearCacheLookup) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");

        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        TxnValueWrapper valueWrapper = txMap.get(keyData);
        if (valueWrapper != null) {
            return (valueWrapper.type != Type.REMOVED);
        }
        return containsKeyInternal(keyData, key, skipNearCacheLookup);
    }

    @Override
    public int size() {
        checkTransactionState();
        int currentSize = sizeInternal();
        for (Map.Entry<Data, TxnValueWrapper> entry : txMap.entrySet()) {
            TxnValueWrapper wrapper = entry.getValue();
            if (wrapper.type == Type.NEW) {
                currentSize++;
            } else if (wrapper.type == Type.REMOVED) {
                VersionedValue versionedValue = valueMap.get(entry.getKey());
                if (versionedValue != null && versionedValue.value != null) {
                    currentSize--;
                }
            }
        }
        return currentSize;
    }

    @Override
    public boolean isEmpty() {
        checkTransactionState();
        return size() == 0;
    }

    @Override
    public V get(Object key) {
        return get(key, false);
    }

    public V get(Object key, boolean skipNearCacheLookup) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");

        Object nearCacheKey = toNearCacheKeyWithStrategy(key);
        Data keyData = mapServiceContext.toData(nearCacheKey, partitionStrategy);
        TxnValueWrapper currentValue = txMap.get(keyData);
        if (currentValue != null) {
            return (V)checkIfRemoved(currentValue);
        }
        return (V)toObjectIfNeeded(getInternal(nearCacheKey, keyData, skipNearCacheLookup));
    }

    @Override
    public V getForUpdate(Object key) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");

        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        TxnValueWrapper currentValue = txMap.get(keyData);
        if (currentValue != null) {
            return (V) checkIfRemoved(currentValue);
        }

        return (V) toObjectIfNeeded(getForUpdateInternal(keyData));
    }

    @Override
    public V put(K key, V value) {
        return put(key, value, -1, MILLISECONDS);
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeUnit) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        Data valueData = mapServiceContext.toData(value);

        NearCachingHook invalidationHook = newNearCachingHook();
        invalidationHook.beforeRemoteCall(key, keyData, value, valueData);

        Object valueBeforeTxn = toObjectIfNeeded(putInternal(keyData, valueData, ttl, timeUnit, invalidationHook));
        TxnValueWrapper currentValue = txMap.get(keyData);
        Type type = valueBeforeTxn == null ? Type.NEW : Type.UPDATED;
        TxnValueWrapper wrapper = new TxnValueWrapper(value, type);
        txMap.put(keyData, wrapper);
        return (V) (currentValue == null ? valueBeforeTxn : checkIfRemoved(currentValue));
    }

    @Override
    public void set(K key, V value) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        Data valueData = mapServiceContext.toData(value);

        NearCachingHook invalidationHook = newNearCachingHook();
        invalidationHook.beforeRemoteCall(key, keyData, value, valueData);

        Data dataBeforeTxn = putInternal(keyData, valueData, UNSET, MILLISECONDS, invalidationHook);
        Type type = dataBeforeTxn == null ? Type.NEW : Type.UPDATED;
        TxnValueWrapper wrapper = new TxnValueWrapper(value, type);
        txMap.put(keyData, wrapper);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        Data valueData = mapServiceContext.toData(value);

        NearCachingHook invalidationHook = newNearCachingHook();
        invalidationHook.beforeRemoteCall(key, keyData, value, valueData);

        TxnValueWrapper wrapper = txMap.get(keyData);
        boolean haveTxnPast = wrapper != null;
        if (haveTxnPast) {
            if (wrapper.type != Type.REMOVED) {
                return (V) wrapper.value;
            }
            putInternal(keyData, valueData, UNSET, MILLISECONDS, invalidationHook);
            txMap.put(keyData, new TxnValueWrapper(value, Type.NEW));
            return null;
        } else {
            Data oldValue = putIfAbsentInternal(keyData, valueData, invalidationHook);
            if (oldValue == null) {
                txMap.put(keyData, new TxnValueWrapper(value, Type.NEW));
            }
            return (V) toObjectIfNeeded(oldValue);
        }
    }

    @Override
    public V replace(K key, V value) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        Data valueData = mapServiceContext.toData(value);

        NearCachingHook invalidationHook = newNearCachingHook();
        invalidationHook.beforeRemoteCall(key, keyData, value, valueData);

        TxnValueWrapper wrapper = txMap.get(keyData);
        boolean haveTxnPast = wrapper != null;
        if (haveTxnPast) {
            if (wrapper.type == Type.REMOVED) {
                return null;
            }
            putInternal(keyData, valueData, UNSET, MILLISECONDS, invalidationHook);
            txMap.put(keyData, new TxnValueWrapper(value, Type.UPDATED));
            return (V) wrapper.value;
        } else {
            Data oldValue = replaceInternal(keyData, valueData, invalidationHook);
            if (oldValue != null) {
                txMap.put(keyData, new TxnValueWrapper(value, Type.UPDATED));
            }
            return (V) toObjectIfNeeded(oldValue);
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");
        checkNotNull(oldValue, "oldValue can't be null");
        checkNotNull(newValue, "newValue can't be null");

        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        Data newValueData = mapServiceContext.toData(newValue);

        NearCachingHook invalidationHook = newNearCachingHook();
        invalidationHook.beforeRemoteCall(key, keyData, newValue, newValueData);

        TxnValueWrapper wrapper = txMap.get(keyData);
        boolean haveTxnPast = wrapper != null;
        if (haveTxnPast) {
            if (!wrapper.value.equals(oldValue)) {
                return false;
            }
            putInternal(keyData, newValueData, UNSET, MILLISECONDS, invalidationHook);
            txMap.put(keyData, new TxnValueWrapper(wrapper.value, Type.UPDATED));
            return true;
        } else {
            boolean success = replaceIfSameInternal(keyData, mapServiceContext.toData(oldValue),
                    newValueData, invalidationHook);
            if (success) {
                txMap.put(keyData, new TxnValueWrapper(newValue, Type.UPDATED));
            }
            return success;
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        TxnValueWrapper wrapper = txMap.get(keyData);
        // wrapper is null which means this entry is not touched by transaction
        if (wrapper == null) {
            NearCachingHook invalidationHook = newNearCachingHook();
            invalidationHook.beforeRemoteCall(key, keyData, null, null);

            boolean removed = removeIfSameInternal(keyData, value, invalidationHook);
            if (removed) {
                txMap.put(keyData, new TxnValueWrapper(value, Type.REMOVED));
            }
            return removed;
        }
        // wrapper type is REMOVED which means entry is already removed inside the transaction
        if (wrapper.type == Type.REMOVED) {
            return false;
        }
        // wrapper value is not equal to passed value
        if (!isEquals(wrapper.value, value)) {
            return false;
        }

        NearCachingHook invalidationHook = newNearCachingHook();
        invalidationHook.beforeRemoteCall(key, keyData, null, null);
        // wrapper value is equal to passed value, we call removeInternal just to add delete log
        removeInternal(keyData, invalidationHook);
        txMap.put(keyData, new TxnValueWrapper(value, Type.REMOVED));

        return true;
    }

    @Override
    public V remove(Object key) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");

        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        NearCachingHook invalidationHook = newNearCachingHook();
        invalidationHook.beforeRemoteCall(key, keyData, null, null);

        Object valueBeforeTxn = toObjectIfNeeded(removeInternal(keyData, invalidationHook));

        TxnValueWrapper wrapper = null;
        if (valueBeforeTxn != null || txMap.containsKey(keyData)) {
            wrapper = txMap.put(keyData, new TxnValueWrapper(valueBeforeTxn, Type.REMOVED));
        }
        return wrapper == null ? (V) valueBeforeTxn : (V) checkIfRemoved(wrapper);
    }

    @Override
    public void delete(Object key) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");

        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        NearCachingHook invalidationHook = newNearCachingHook();
        invalidationHook.beforeRemoteCall(key, keyData, null, null);

        Data data = removeInternal(keyData, invalidationHook);
        if (data != null || txMap.containsKey(keyData)) {
            txMap.put(keyData, new TxnValueWrapper(toObjectIfNeeded(data), Type.REMOVED));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet() {
        return keySet(Predicates.alwaysTrue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet(Predicate<K,V> predicate) {
        checkTransactionState();
        checkNotNull(predicate, "Predicate should not be null!");
        checkNotInstanceOf(PagingPredicate.class, predicate, "Paging is not supported for Transactional queries!");

        QueryEngine queryEngine = mapServiceContext.getQueryEngine(name);

        Query query = Query.of().mapName(name).predicate(predicate).iterationType(IterationType.KEY).build();
        QueryResult queryResult = queryEngine.execute(query, Target.ALL_NODES);
        Set queryResultSet = QueryResultUtils.transformToSet(ss, queryResult,
                predicate, IterationType.KEY, true, tx.isOriginatedFromClient());

        Extractors extractors = mapServiceContext.getExtractors(name);
        Set<K> returningKeySet = new HashSet<K>(queryResultSet);
        CachedQueryEntry cachedQueryEntry = new CachedQueryEntry();
        for (Map.Entry<Data, TxnValueWrapper> entry : txMap.entrySet()) {
            if (entry.getValue().type == Type.REMOVED) {
                // meanwhile remove keys which are not in txMap
                returningKeySet.remove(toObjectIfNeeded(entry.getKey()));
            } else {
                Data keyData = entry.getKey();

                if (predicate == Predicates.alwaysTrue()) {
                    returningKeySet.add((K)toObjectIfNeeded(keyData));
                } else {
                    cachedQueryEntry.init(ss, keyData, entry.getValue().value, extractors);
                    // apply predicate on txMap
                    if (predicate.apply(cachedQueryEntry)) {
                        returningKeySet.add((K)toObjectIfNeeded(keyData));
                    }
                }
            }
        }
        return returningKeySet;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<V> values() {
        return values(Predicates.alwaysTrue());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<V> values(Predicate<K,V> predicate) {
        checkTransactionState();
        checkNotNull(predicate, "Predicate can not be null!");
        checkNotInstanceOf(PagingPredicate.class, predicate, "Paging is not supported for Transactional queries");

        QueryEngine queryEngine = mapServiceContext.getQueryEngine(name);

        Query query = Query.of().mapName(name).predicate(predicate).iterationType(IterationType.ENTRY).build();
        QueryResult queryResult = queryEngine.execute(query, Target.ALL_NODES);
        Set result = QueryResultUtils.transformToSet(ss, queryResult,
                predicate, IterationType.ENTRY, true, true);

        // TODO: can't we just use the original set?
        List<V> valueSet = new ArrayList<>();
        Set<Data> keyWontBeIncluded = new HashSet<>();

        Extractors extractors = mapServiceContext.getExtractors(name);
        CachedQueryEntry cachedQueryEntry = new CachedQueryEntry();
        // iterate over the txMap and see if the values are updated or removed
        for (Map.Entry<Data, TxnValueWrapper> entry : txMap.entrySet()) {
            boolean isRemoved = Type.REMOVED.equals(entry.getValue().type);
            boolean isUpdated = Type.UPDATED.equals(entry.getValue().type);

            if (isRemoved) {
                keyWontBeIncluded.add(entry.getKey());
            } else {
                if (isUpdated) {
                    keyWontBeIncluded.add(entry.getKey());
                }
                Object entryValue = entry.getValue().value;
                cachedQueryEntry.init(ss, entry.getKey(), entryValue, extractors);
                if (predicate.apply(cachedQueryEntry)) {
                    valueSet.add((V)toObjectIfNeeded(cachedQueryEntry.getValueData()));
                }
            }
        }
        removeFromResultSet(result, valueSet, keyWontBeIncluded);
        return valueSet;
    }

    @Override
    public String toString() {
        return "TransactionalMap" + "{name='" + name + '\'' + '}';
    }

    private Object checkIfRemoved(TxnValueWrapper wrapper) {
        checkTransactionState();
        return wrapper == null || wrapper.type == Type.REMOVED ? null : wrapper.value;
    }

    private void removeFromResultSet(Set<Map.Entry<K,V>> queryResultSet, List<V> valueSet,
                                     Set<Data> keyWontBeIncluded) {
        for (Map.Entry<K,V> entry : queryResultSet) {
            if (keyWontBeIncluded.contains(entry.getKey())) {
                continue;
            }
            valueSet.add((V)toObjectIfNeeded(entry.getValue()));
        }
    }
}
