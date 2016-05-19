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

package com.hazelcast.map.impl.tx;

import com.hazelcast.core.TransactionalMap;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.query.MapQueryEngine;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultCollection;
import com.hazelcast.map.impl.tx.TxnValueWrapper.Type;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.util.IterationType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotInstanceOf;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Proxy implementation of {@link com.hazelcast.core.TransactionalMap} interface.
 */
public class TransactionalMapProxy<K, V> extends TransactionalMapProxySupport implements TransactionalMap<K, V> {

    private final Map<Data, TxnValueWrapper<V>> txMap = new HashMap<Data, TxnValueWrapper<V>>();

    public TransactionalMapProxy(String name, MapService mapService, NodeEngine nodeEngine, Transaction transaction) {
        super(name, mapService, nodeEngine, transaction);
    }

    @Override
    public boolean containsKey(Object key) {
        checkTransactionState();
        Data keyData = getService().getMapServiceContext().toData(key, partitionStrategy);

        TxnValueWrapper<V> valueWrapper = txMap.get(keyData);
        if (valueWrapper != null) {
            return (valueWrapper.type != Type.REMOVED);
        }
        return containsKeyInternal(keyData);
    }

    @Override
    public int size() {
        checkTransactionState();
        int currentSize = sizeInternal();
        for (Map.Entry<Data, TxnValueWrapper<V>> entry : txMap.entrySet()) {
            TxnValueWrapper<V> wrapper = entry.getValue();
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
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        TxnValueWrapper<V> currentValue = txMap.get(keyData);
        if (currentValue != null) {
            return checkIfRemoved(currentValue);
        }
        return toObjectIfNeeded(getInternal(keyData));
    }

    @Override
    public V getForUpdate(Object key) {
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        TxnValueWrapper<V> currentValue = txMap.get(keyData);
        if (currentValue != null) {
            return checkIfRemoved(currentValue);
        }

        return toObjectIfNeeded(getForUpdateInternal(keyData));
    }

    @Override
    public V put(K key, V value) {
        return put(key, value, -1, MILLISECONDS);
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeUnit) {
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        V valueBeforeTxn = toObjectIfNeeded(putInternal(keyData, mapServiceContext.toData(value), ttl, timeUnit));

        TxnValueWrapper<V> currentValue = txMap.get(keyData);
        if (value != null) {
            Type type = valueBeforeTxn == null ? Type.NEW : Type.UPDATED;
            TxnValueWrapper<V> wrapper = new TxnValueWrapper<V>(value, type);
            txMap.put(keyData, wrapper);
        }
        return currentValue == null ? valueBeforeTxn : checkIfRemoved(currentValue);
    }

    @Override
    public void set(K key, V value) {
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        Data dataBeforeTxn = putInternal(keyData, mapServiceContext.toData(value), -1, MILLISECONDS);
        if (value != null) {
            Type type = dataBeforeTxn == null ? Type.NEW : Type.UPDATED;
            TxnValueWrapper<V> wrapper = new TxnValueWrapper<V>(value, type);
            txMap.put(keyData, wrapper);
        }
    }

    @Override
    public V putIfAbsent(K key, V value) {
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        TxnValueWrapper<V> wrapper = txMap.get(keyData);
        boolean haveTxnPast = wrapper != null;
        if (haveTxnPast) {
            if (wrapper.type != Type.REMOVED) {
                return wrapper.value;
            }
            putInternal(keyData, mapServiceContext.toData(value), -1, MILLISECONDS);
            txMap.put(keyData, new TxnValueWrapper<V>(value, Type.NEW));
            return null;
        } else {
            Data oldValue
                    = putIfAbsentInternal(keyData,
                    mapServiceContext.toData(value));
            if (oldValue == null) {
                txMap.put(keyData, new TxnValueWrapper<V>(value, Type.NEW));
            }
            return toObjectIfNeeded(oldValue);
        }
    }

    @Override
    public V replace(K key, V value) {
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        TxnValueWrapper<V> wrapper = txMap.get(keyData);
        boolean haveTxnPast = wrapper != null;
        if (haveTxnPast) {
            if (wrapper.type == Type.REMOVED) {
                return null;
            }
            putInternal(keyData, mapServiceContext.toData(value), -1, MILLISECONDS);
            txMap.put(keyData, new TxnValueWrapper<V>(value, Type.UPDATED));
            return wrapper.value;
        } else {
            Data oldValue = replaceInternal(keyData, mapServiceContext.toData(value));
            if (oldValue != null) {
                txMap.put(keyData, new TxnValueWrapper<V>(value, Type.UPDATED));
            }
            return toObjectIfNeeded(oldValue);
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        TxnValueWrapper<V> wrapper = txMap.get(keyData);
        boolean haveTxnPast = wrapper != null;
        if (haveTxnPast) {
            if (!wrapper.value.equals(oldValue)) {
                return false;
            }
            putInternal(keyData, mapServiceContext.toData(newValue), -1, MILLISECONDS);
            txMap.put(keyData, new TxnValueWrapper<V>(wrapper.value, Type.UPDATED));
            return true;
        } else {
            boolean success = replaceIfSameInternal(keyData,
                    mapServiceContext.toData(oldValue), mapServiceContext.toData(newValue));
            if (success) {
                txMap.put(keyData, new TxnValueWrapper<V>(newValue, Type.UPDATED));
            }
            return success;
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        TxnValueWrapper<V> wrapper = txMap.get(keyData);
        if (wrapper != null && !isEquals(wrapper.value, value)) {
            return false;
        }

        boolean removed = removeIfSameInternal(keyData, value);
        if (removed) {
            txMap.put(keyData, new TxnValueWrapper<V>((V) value, Type.REMOVED));
        }
        return removed;
    }

    @Override
    public V remove(Object key) {
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        V valueBeforeTxn = toObjectIfNeeded(removeInternal(keyData));

        TxnValueWrapper<V> wrapper = null;
        if (valueBeforeTxn != null || txMap.containsKey(keyData)) {
            wrapper = txMap.put(keyData, new TxnValueWrapper<V>(valueBeforeTxn, Type.REMOVED));
        }
        return wrapper == null ? valueBeforeTxn : checkIfRemoved(wrapper);
    }

    @Override
    public void delete(Object key) {
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();

        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        Data data = removeInternal(keyData);
        if (data != null || txMap.containsKey(keyData)) {
            V value = toObjectIfNeeded(data);
            txMap.put(keyData, new TxnValueWrapper<V>(value, Type.REMOVED));
        }
    }

    @Override
    public Set<K> keySet() {
        return keySet(TruePredicate.INSTANCE);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet(Predicate predicate) {
        checkTransactionState();
        checkNotNull(predicate, "Predicate should not be null!");
        checkNotInstanceOf(PagingPredicate.class, predicate, "Paging is not supported for Transactional queries!");

        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        MapQueryEngine queryEngine = mapServiceContext.getMapQueryEngine(name);
        SerializationService serializationService = getNodeEngine().getSerializationService();

        QueryResult result = queryEngine.invokeQueryAllPartitions(name, predicate, IterationType.KEY);
        Set<K> queryResult = new QueryResultCollection<K>(serializationService, IterationType.KEY, false, true, result);

        // TODO: Can't we just use the original set?
        Set<K> keySet = new HashSet<K>(queryResult);
        Extractors extractors = mapServiceContext.getExtractors(name);
        for (Map.Entry<Data, TxnValueWrapper<V>> entry : txMap.entrySet()) {
            Data keyData = entry.getKey();
            if (!Type.REMOVED.equals(entry.getValue().type)) {
                V value = (entry.getValue().value instanceof Data)
                        ? (V) toObjectIfNeeded(entry.getValue().value) : entry.getValue().value;

                QueryableEntry queryEntry = new CachedQueryEntry((InternalSerializationService) serializationService,
                        keyData, value, extractors);
                QueryableEntry<K, V> queryEntry = new CachedQueryEntry<K, V>((InternalSerializationService) serializationService, 
                        keyData, value, extractors);
                // apply predicate on txMap
                if (predicate.apply(queryEntry)) {
                    K keyObject = serializationService.toObject(keyData);
                    keySet.add(keyObject);
                }
            } else {
                // meanwhile remove keys which are not in txMap
                Object keyObject = serializationService.toObject(keyData);
                keySet.remove(keyObject);
            }
        }
        return keySet;
    }

    @Override
    public Collection<V> values() {
        return values(TruePredicate.INSTANCE);
    }

    @Override
    public Collection<V> values(Predicate predicate) {
        checkTransactionState();
        checkNotNull(predicate, "Predicate can not be null!");
        checkNotInstanceOf(PagingPredicate.class, predicate, "Paging is not supported for Transactional queries");

        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        MapQueryEngine queryEngine = mapServiceContext.getMapQueryEngine(name);
        SerializationService serializationService = getNodeEngine().getSerializationService();

        QueryResult result = queryEngine.invokeQueryAllPartitions(name, predicate, IterationType.ENTRY);
        QueryResultCollection<Map.Entry<K, V>> queryResult
                = new QueryResultCollection<Map.Entry<K, V>>(serializationService, IterationType.ENTRY, false, true, result);

        // TODO: Can't we just use the original set?
        List<V> valueSet = new ArrayList<V>();
        Set<K> keyWontBeIncluded = new HashSet<K>();
        Extractors extractors = mapServiceContext.getExtractors(name);

        // iterate over the txMap and see if the values are updated or removed
        for (Map.Entry<Data, TxnValueWrapper<V>> entry : txMap.entrySet()) {
            boolean isRemoved = Type.REMOVED.equals(entry.getValue().type);
            boolean isUpdated = Type.UPDATED.equals(entry.getValue().type);

            K keyObject = serializationService.toObject(entry.getKey());
            if (isRemoved) {
                keyWontBeIncluded.add(keyObject);
            } else {
                if (isUpdated) {
                    keyWontBeIncluded.add(keyObject);
                }
                V entryValue = entry.getValue().value;
                QueryableEntry<K, V> queryEntry = new CachedQueryEntry<K, V>((InternalSerializationService) serializationService,
                        entry.getKey(), entryValue, extractors);
                if (predicate.apply(queryEntry)) {
                    valueSet.add(queryEntry.getValue());
                }
            }
        }
        removeFromResultSet(queryResult, valueSet, keyWontBeIncluded);
        return valueSet;
    }

    @Override
    public String toString() {
        return "TransactionalMap" + "{name='" + name + '\'' + '}';
    }

    private V checkIfRemoved(TxnValueWrapper<V> wrapper) {
        checkTransactionState();
        return wrapper == null || wrapper.type == Type.REMOVED ? null : wrapper.value;
    }

    private void removeFromResultSet(QueryResultCollection<Map.Entry<K, V>> queryResultSet, List<V> valueSet,
                                     Set<K> keyWontBeIncluded) {
        for (Map.Entry<K, V> entry : queryResultSet) {
            if (keyWontBeIncluded.contains(entry.getKey())) {
                continue;
            }
            valueSet.add(entry.getValue());
        }
    }
}
