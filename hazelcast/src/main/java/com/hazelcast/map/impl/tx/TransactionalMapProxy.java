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
public class TransactionalMapProxy extends TransactionalMapProxySupport implements TransactionalMap {

    private final Map<Data, TxnValueWrapper> txMap = new HashMap<Data, TxnValueWrapper>();

    public TransactionalMapProxy(String name, MapService mapService, NodeEngine nodeEngine, Transaction transaction) {
        super(name, mapService, nodeEngine, transaction);
    }

    @Override
    public boolean containsKey(Object key) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");

        Data keyData = getService().getMapServiceContext().toData(key, partitionStrategy);
        TxnValueWrapper valueWrapper = txMap.get(keyData);
        if (valueWrapper != null) {
            return (valueWrapper.type != Type.REMOVED);
        }
        return containsKeyInternal(keyData);
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

        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        TxnValueWrapper currentValue = txMap.get(keyData);
        if (currentValue != null) {
            return checkIfRemoved(currentValue);
        }
        return toObjectIfNeeded(getInternal(keyData));
    }

    @Override
    public Object getForUpdate(Object key) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");

        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        TxnValueWrapper currentValue = txMap.get(keyData);
        if (currentValue != null) {
            return checkIfRemoved(currentValue);
        }

        return toObjectIfNeeded(getForUpdateInternal(keyData));
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

        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        Object valueBeforeTxn = toObjectIfNeeded(putInternal(keyData, mapServiceContext.toData(value), ttl, timeUnit));

        TxnValueWrapper currentValue = txMap.get(keyData);
        Type type = valueBeforeTxn == null ? Type.NEW : Type.UPDATED;
        TxnValueWrapper wrapper = new TxnValueWrapper(value, type);
        txMap.put(keyData, wrapper);
        return currentValue == null ? valueBeforeTxn : checkIfRemoved(currentValue);
    }

    @Override
    public void set(Object key, Object value) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        Data dataBeforeTxn = putInternal(keyData, mapServiceContext.toData(value), -1, MILLISECONDS);
        Type type = dataBeforeTxn == null ? Type.NEW : Type.UPDATED;
        TxnValueWrapper wrapper = new TxnValueWrapper(value, type);
        txMap.put(keyData, wrapper);
    }

    @Override
    public Object putIfAbsent(Object key, Object value) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        TxnValueWrapper wrapper = txMap.get(keyData);
        boolean haveTxnPast = wrapper != null;
        if (haveTxnPast) {
            if (wrapper.type != Type.REMOVED) {
                return wrapper.value;
            }
            putInternal(keyData, mapServiceContext.toData(value), -1, MILLISECONDS);
            txMap.put(keyData, new TxnValueWrapper(value, Type.NEW));
            return null;
        } else {
            Data oldValue
                    = putIfAbsentInternal(keyData,
                    mapServiceContext.toData(value));
            if (oldValue == null) {
                txMap.put(keyData, new TxnValueWrapper(value, Type.NEW));
            }
            return toObjectIfNeeded(oldValue);
        }
    }

    @Override
    public Object replace(Object key, Object value) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        TxnValueWrapper wrapper = txMap.get(keyData);
        boolean haveTxnPast = wrapper != null;
        if (haveTxnPast) {
            if (wrapper.type == Type.REMOVED) {
                return null;
            }
            putInternal(keyData, mapServiceContext.toData(value), -1, MILLISECONDS);
            txMap.put(keyData, new TxnValueWrapper(value, Type.UPDATED));
            return wrapper.value;
        } else {
            Data oldValue = replaceInternal(keyData, mapServiceContext.toData(value));
            if (oldValue != null) {
                txMap.put(keyData, new TxnValueWrapper(value, Type.UPDATED));
            }
            return toObjectIfNeeded(oldValue);
        }
    }

    @Override
    public boolean replace(Object key, Object oldValue, Object newValue) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");
        checkNotNull(oldValue, "oldValue can't be null");
        checkNotNull(newValue, "newValue can't be null");

        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        TxnValueWrapper wrapper = txMap.get(keyData);
        boolean haveTxnPast = wrapper != null;
        if (haveTxnPast) {
            if (!wrapper.value.equals(oldValue)) {
                return false;
            }
            putInternal(keyData, mapServiceContext.toData(newValue), -1, MILLISECONDS);
            txMap.put(keyData, new TxnValueWrapper(wrapper.value, Type.UPDATED));
            return true;
        } else {
            boolean success = replaceIfSameInternal(keyData,
                    mapServiceContext.toData(oldValue), mapServiceContext.toData(newValue));
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

        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        TxnValueWrapper wrapper = txMap.get(keyData);
        //Wrapper is null which means this entry is not touched by transaction
        if (wrapper == null) {
            boolean removed = removeIfSameInternal(keyData, value);
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
        // wrapper value is equal to passed value, we call removeInternal just to add delete log
        removeInternal(keyData);
        txMap.put(keyData, new TxnValueWrapper(value, Type.REMOVED));
        return true;
    }

    @Override
    public Object remove(Object key) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");

        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        Object valueBeforeTxn = toObjectIfNeeded(removeInternal(keyData));

        TxnValueWrapper wrapper = null;
        if (valueBeforeTxn != null || txMap.containsKey(keyData)) {
            wrapper = txMap.put(keyData, new TxnValueWrapper(valueBeforeTxn, Type.REMOVED));
        }
        return wrapper == null ? valueBeforeTxn : checkIfRemoved(wrapper);
    }

    @Override
    public void delete(Object key) {
        checkTransactionState();
        checkNotNull(key, "key can't be null");

        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();

        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        Data data = removeInternal(keyData);
        if (data != null || txMap.containsKey(keyData)) {
            txMap.put(keyData, new TxnValueWrapper(toObjectIfNeeded(data), Type.REMOVED));
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

        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        MapQueryEngine queryEngine = mapServiceContext.getMapQueryEngine(name);
        SerializationService serializationService = getNodeEngine().getSerializationService();

        QueryResult result = queryEngine.invokeQueryAllPartitions(name, predicate, IterationType.KEY);
        Set<Object> queryResult = new QueryResultCollection(serializationService, IterationType.KEY, false, true, result);

        // TODO: Can't we just use the original set?
        Set<Object> keySet = new HashSet<Object>(queryResult);
        Extractors extractors = mapServiceContext.getExtractors(name);
        for (Map.Entry<Data, TxnValueWrapper> entry : txMap.entrySet()) {
            Data keyData = entry.getKey();
            if (!Type.REMOVED.equals(entry.getValue().type)) {
                Object value = (entry.getValue().value instanceof Data)
                        ? toObjectIfNeeded(entry.getValue().value) : entry.getValue().value;

                QueryableEntry queryEntry = new CachedQueryEntry((InternalSerializationService) serializationService,
                        keyData, value, extractors);
                // apply predicate on txMap
                if (predicate.apply(queryEntry)) {
                    Object keyObject = serializationService.toObject(keyData);
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

        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        MapQueryEngine queryEngine = mapServiceContext.getMapQueryEngine(name);
        SerializationService serializationService = getNodeEngine().getSerializationService();

        QueryResult result = queryEngine.invokeQueryAllPartitions(name, predicate, IterationType.ENTRY);
        QueryResultCollection<Map.Entry> queryResult
                = new QueryResultCollection<Map.Entry>(serializationService, IterationType.ENTRY, false, true, result);

        // TODO: Can't we just use the original set?
        List<Object> valueSet = new ArrayList<Object>();
        Set<Object> keyWontBeIncluded = new HashSet<Object>();
        Extractors extractors = mapServiceContext.getExtractors(name);

        // iterate over the txMap and see if the values are updated or removed
        for (Map.Entry<Data, TxnValueWrapper> entry : txMap.entrySet()) {
            boolean isRemoved = Type.REMOVED.equals(entry.getValue().type);
            boolean isUpdated = Type.UPDATED.equals(entry.getValue().type);

            Object keyObject = serializationService.toObject(entry.getKey());
            if (isRemoved) {
                keyWontBeIncluded.add(keyObject);
            } else {
                if (isUpdated) {
                    keyWontBeIncluded.add(keyObject);
                }
                Object entryValue = entry.getValue().value;
                QueryableEntry queryEntry = new CachedQueryEntry((InternalSerializationService) serializationService,
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

    private Object checkIfRemoved(TxnValueWrapper wrapper) {
        checkTransactionState();
        return wrapper == null || wrapper.type == Type.REMOVED ? null : wrapper.value;
    }

    private void removeFromResultSet(QueryResultCollection<Map.Entry> queryResultSet, List<Object> valueSet,
                                     Set<Object> keyWontBeIncluded) {
        for (Map.Entry entry : queryResultSet) {
            if (keyWontBeIncluded.contains(entry.getKey())) {
                continue;
            }
            valueSet.add(entry.getValue());
        }
    }
}
