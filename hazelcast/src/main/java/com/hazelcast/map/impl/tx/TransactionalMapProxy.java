/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.query.MapQueryEngine;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultUtils;
import com.hazelcast.map.impl.query.Target;
import com.hazelcast.map.impl.tx.TxnValueWrapper.Type;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.NodeEngine;
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
 * Proxy implementation of {@link com.hazelcast.core.TransactionalMap}
 * interface.
 */
public class TransactionalMapProxy extends TransactionalMapProxySupport implements TransactionalMap {

    private final Map<Data, TxnValueWrapper> txMap = new HashMap<Data, TxnValueWrapper>();

    public TransactionalMapProxy(String name, MapService mapService, NodeEngine nodeEngine, Transaction transaction) {
        super(name, mapService, nodeEngine, transaction);
    }

    @Override
    public boolean containsKey(Object key) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");

        Data dataKey = ss.toData(key, partitionStrategy);

        TxnValueWrapper valueWrapper = txMap.get(dataKey);
        if (valueWrapper != null) {
            return (valueWrapper.type != Type.REMOVED);
        }
        return containsKeyInternal(key, dataKey);
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
    public Object get(Object key) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");

        Data dataKey = ss.toData(key, partitionStrategy);

        TxnValueWrapper currentValue = txMap.get(dataKey);
        if (currentValue != null) {
            return checkIfRemoved(currentValue);
        }
        return toObjectIfNeeded(getInternal(key, dataKey));
    }

    @Override
    public Object getForUpdate(Object key) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");

        Data dataKey = ss.toData(key, partitionStrategy);

        TxnValueWrapper currentValue = txMap.get(dataKey);
        if (currentValue != null) {
            return checkIfRemoved(currentValue);
        }

        return toObjectIfNeeded(getForUpdateInternal(dataKey));
    }

    @Override
    public Object put(Object key, Object value) {
        return put(key, value, -1, MILLISECONDS);
    }

    @Override
    public Object put(Object key, Object value, long ttl, TimeUnit timeUnit) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        Data dataKey = ss.toData(key, partitionStrategy);
        try {
            Object valueBeforeTxn = toObjectIfNeeded(putInternal(dataKey, ss.toData(value), ttl, timeUnit));

            TxnValueWrapper currentValue = txMap.get(dataKey);
            Type type = valueBeforeTxn == null ? Type.NEW : Type.UPDATED;
            TxnValueWrapper wrapper = new TxnValueWrapper(value, type);
            txMap.put(dataKey, wrapper);
            return currentValue == null ? valueBeforeTxn : checkIfRemoved(currentValue);
        } finally {
            invalidateNearCache(key, dataKey);
        }
    }

    @Override
    public void set(Object key, Object value) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        Data dataKey = ss.toData(key, partitionStrategy);
        try {
            Data dataBeforeTxn = putInternal(dataKey, ss.toData(value), -1, MILLISECONDS);
            Type type = dataBeforeTxn == null ? Type.NEW : Type.UPDATED;
            TxnValueWrapper wrapper = new TxnValueWrapper(value, type);
            txMap.put(dataKey, wrapper);
        } finally {
            invalidateNearCache(key, dataKey);
        }
    }

    @Override
    public Object putIfAbsent(Object key, Object value) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        Data dataKey = ss.toData(key, partitionStrategy);
        try {
            TxnValueWrapper wrapper = txMap.get(dataKey);
            boolean haveTxnPast = wrapper != null;
            if (haveTxnPast) {
                if (wrapper.type != Type.REMOVED) {
                    return wrapper.value;
                }
                putInternal(dataKey, ss.toData(value), -1, MILLISECONDS);
                txMap.put(dataKey, new TxnValueWrapper(value, Type.NEW));
                return null;
            } else {
                Data oldValue = putIfAbsentInternal(dataKey, ss.toData(value));
                if (oldValue == null) {
                    txMap.put(dataKey, new TxnValueWrapper(value, Type.NEW));
                }
                return toObjectIfNeeded(oldValue);
            }
        } finally {
            invalidateNearCache(key, dataKey);
        }
    }

    @Override
    public Object replace(Object key, Object value) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        Data dataKey = ss.toData(key, partitionStrategy);
        try {
            TxnValueWrapper wrapper = txMap.get(dataKey);
            boolean haveTxnPast = wrapper != null;
            if (haveTxnPast) {
                if (wrapper.type == Type.REMOVED) {
                    return null;
                }
                putInternal(dataKey, ss.toData(value), -1, MILLISECONDS);
                txMap.put(dataKey, new TxnValueWrapper(value, Type.UPDATED));
                return wrapper.value;
            } else {
                Data oldValue = replaceInternal(dataKey, ss.toData(value));
                if (oldValue != null) {
                    txMap.put(dataKey, new TxnValueWrapper(value, Type.UPDATED));
                }
                return toObjectIfNeeded(oldValue);
            }
        } finally {
            invalidateNearCache(key, dataKey);
        }
    }

    @Override
    public boolean replace(Object key, Object oldValue, Object newValue) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");
        checkNotNull(oldValue, "oldValue can't be null");
        checkNotNull(newValue, "newValue can't be null");

        Data dataKey = ss.toData(key, partitionStrategy);
        try {
            TxnValueWrapper wrapper = txMap.get(dataKey);
            boolean haveTxnPast = wrapper != null;
            if (haveTxnPast) {
                if (!wrapper.value.equals(oldValue)) {
                    return false;
                }
                putInternal(dataKey, ss.toData(newValue), -1, MILLISECONDS);
                txMap.put(dataKey, new TxnValueWrapper(wrapper.value, Type.UPDATED));
                return true;
            } else {
                boolean success = replaceIfSameInternal(dataKey, ss.toData(oldValue), ss.toData(newValue));
                if (success) {
                    txMap.put(dataKey, new TxnValueWrapper(newValue, Type.UPDATED));
                }
                return success;
            }
        } finally {
            invalidateNearCache(key, dataKey);
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        Data dataKey = ss.toData(key, partitionStrategy);
        try {
            TxnValueWrapper wrapper = txMap.get(dataKey);
            // wrapper is null which means this entry is not touched by transaction
            if (wrapper == null) {
                boolean removed = removeIfSameInternal(dataKey, value);
                if (removed) {
                    txMap.put(dataKey, new TxnValueWrapper(value, Type.REMOVED));
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
            // wrapper value is equal to passed value, we call removeInternal just to add delete log
            removeInternal(dataKey);
            txMap.put(dataKey, new TxnValueWrapper(value, Type.REMOVED));
            return true;
        } finally {
            invalidateNearCache(key, dataKey);
        }
    }

    @Override
    public Object remove(Object key) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");

        Data dataKey = ss.toData(key, partitionStrategy);
        try {
            Object valueBeforeTxn = toObjectIfNeeded(removeInternal(dataKey));

            TxnValueWrapper wrapper = null;
            if (valueBeforeTxn != null || txMap.containsKey(dataKey)) {
                wrapper = txMap.put(dataKey, new TxnValueWrapper(valueBeforeTxn, Type.REMOVED));
            }
            return wrapper == null ? valueBeforeTxn : checkIfRemoved(wrapper);
        } finally {
            invalidateNearCache(key, dataKey);
        }
    }

    @Override
    public void delete(Object key) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");

        Data dataKey = ss.toData(key, partitionStrategy);
        try {
            Data data = removeInternal(dataKey);
            if (data != null || txMap.containsKey(dataKey)) {
                txMap.put(dataKey, new TxnValueWrapper(toObjectIfNeeded(data), Type.REMOVED));
            }
        } finally {
            invalidateNearCache(key, dataKey);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<Object> keySet() {
        return keySet(TruePredicate.INSTANCE);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set keySet(Predicate predicate) {
        checkTransactionState();
        checkNotNull(predicate, "Predicate should not be null!");
        checkNotInstanceOf(PagingPredicate.class, predicate, "Paging is not supported for Transactional queries!");

        MapQueryEngine queryEngine = mapServiceContext.getMapQueryEngine(name);

        Query query = Query.of().mapName(name).predicate(predicate).iterationType(IterationType.KEY).build();
        QueryResult queryResult = queryEngine.execute(query, Target.ALL_NODES);
        Set queryResultSet = QueryResultUtils.transformToSet(ss, queryResult,
                predicate, IterationType.KEY, true, tx.isOriginatedFromClient());

        Extractors extractors = mapServiceContext.getExtractors(name);
        Set<Object> returningKeySet = new HashSet<Object>(queryResultSet);
        CachedQueryEntry cachedQueryEntry = new CachedQueryEntry();
        for (Map.Entry<Data, TxnValueWrapper> entry : txMap.entrySet()) {
            if (entry.getValue().type == Type.REMOVED) {
                // meanwhile remove keys which are not in txMap
                returningKeySet.remove(toObjectIfNeeded(entry.getKey()));
            } else {
                Data dataKey = entry.getKey();

                if (predicate == TruePredicate.INSTANCE) {
                    returningKeySet.add(toObjectIfNeeded(dataKey));
                } else {
                    cachedQueryEntry.init(ss, dataKey, entry.getValue().value, extractors);
                    // apply predicate on txMap
                    if (predicate.apply(cachedQueryEntry)) {
                        returningKeySet.add(toObjectIfNeeded(dataKey));
                    }
                }
            }
        }
        return returningKeySet;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<Object> values() {
        return values(TruePredicate.INSTANCE);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection values(Predicate predicate) {
        checkTransactionState();
        checkNotNull(predicate, "Predicate can not be null!");
        checkNotInstanceOf(PagingPredicate.class, predicate, "Paging is not supported for Transactional queries");

        MapQueryEngine queryEngine = mapServiceContext.getMapQueryEngine(name);

        Query query = Query.of().mapName(name).predicate(predicate).iterationType(IterationType.ENTRY).build();
        QueryResult queryResult = queryEngine.execute(query, Target.ALL_NODES);
        Set result = QueryResultUtils.transformToSet(ss, queryResult,
                predicate, IterationType.ENTRY, true, true);

        // TODO: can't we just use the original set?
        List<Object> valueSet = new ArrayList<Object>();
        Set<Data> keyWontBeIncluded = new HashSet<Data>();

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
                    valueSet.add(toObjectIfNeeded(cachedQueryEntry.getValueData()));
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

    private void removeFromResultSet(Set<Map.Entry> queryResultSet, List<Object> valueSet,
                                     Set<Data> keyWontBeIncluded) {
        for (Map.Entry entry : queryResultSet) {
            if (keyWontBeIncluded.contains((Data) entry.getKey())) {
                continue;
            }
            valueSet.add(toObjectIfNeeded(entry.getValue()));
        }
    }
}
