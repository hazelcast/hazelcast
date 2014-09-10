/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.QueryResultSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Proxy implementation of {@link com.hazelcast.core.TransactionalMap} interface.
 */
public class TransactionalMapProxy extends TransactionalMapProxySupport implements TransactionalMap {

    private final Map<Object, TxnValueWrapper> txMap = new HashMap<Object, TxnValueWrapper>();

    public TransactionalMapProxy(String name, MapService mapService, NodeEngine nodeEngine, TransactionSupport transaction) {
        super(name, mapService, nodeEngine, transaction);
    }

    public boolean containsKey(Object key) {
        checkTransactionState();
        final TxnValueWrapper valueWrapper = txMap.get(key);
        if (valueWrapper != null) {
            return valueWrapper.type == TxnValueWrapper.Type.REMOVED ? false : true;
        }
        return containsKeyInternal(getService().getMapServiceContext().toData(key, partitionStrategy));
    }

    public int size() {
        checkTransactionState();
        int currentSize = sizeInternal();
        for (TxnValueWrapper wrapper : txMap.values()) {
            if (wrapper.type == TxnValueWrapper.Type.NEW) {
                currentSize++;
            } else if (wrapper.type == TxnValueWrapper.Type.REMOVED) {
                currentSize--;
            }
        }
        return currentSize;
    }

    public boolean isEmpty() {
        checkTransactionState();
        return size() == 0;
    }

    public Object get(Object key) {
        checkTransactionState();
        TxnValueWrapper currentValue = txMap.get(key);
        if (currentValue != null) {
            return checkIfRemoved(currentValue);
        }
        final MapService service = getService();
        final MapServiceContext mapServiceContext = service.getMapServiceContext();
        return mapServiceContext.toObject(getInternal(mapServiceContext.toData(key, partitionStrategy)));
    }

    public Object getForUpdate(Object key) {
        checkTransactionState();
        TxnValueWrapper currentValue = txMap.get(key);
        if (currentValue != null) {
            return checkIfRemoved(currentValue);
        }
        final MapService service = getService();
        final MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data dataKey = mapServiceContext.toData(key, partitionStrategy);
        return mapServiceContext.toObject(getForUpdateInternal(dataKey));
    }

    private Object checkIfRemoved(TxnValueWrapper wrapper) {
        checkTransactionState();
        return wrapper == null || wrapper.type == TxnValueWrapper.Type.REMOVED ? null : wrapper.value;
    }

    public Object put(Object key, Object value) {
        checkTransactionState();
        MapService service = getService();
        final MapServiceContext mapServiceContext = service.getMapServiceContext();
        final Object valueBeforeTxn = mapServiceContext.toObject(putInternal(mapServiceContext.toData(key, partitionStrategy),
                mapServiceContext.toData(value)));
        TxnValueWrapper currentValue = txMap.get(key);
        if (value != null) {
            TxnValueWrapper wrapper = valueBeforeTxn == null
                    ? new TxnValueWrapper(value, TxnValueWrapper.Type.NEW)
                    : new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED);

            txMap.put(key, wrapper);
        }
        return currentValue == null ? valueBeforeTxn : checkIfRemoved(currentValue);
    }

    public Object put(Object key, Object value, long ttl, TimeUnit timeUnit) {
        checkTransactionState();
        MapService service = getService();
        final MapServiceContext mapServiceContext = service.getMapServiceContext();
        final Object valueBeforeTxn = mapServiceContext.toObject(putInternal(mapServiceContext.toData(key, partitionStrategy),
                mapServiceContext.toData(value), ttl, timeUnit));
        TxnValueWrapper currentValue = txMap.get(key);
        if (value != null) {
            TxnValueWrapper wrapper = valueBeforeTxn == null
                    ? new TxnValueWrapper(value, TxnValueWrapper.Type.NEW)
                    : new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED);
            txMap.put(key, wrapper);
        }
        return currentValue == null ? valueBeforeTxn : checkIfRemoved(currentValue);
    }

    public void set(Object key, Object value) {
        checkTransactionState();
        MapService service = getService();
        final MapServiceContext mapServiceContext = service.getMapServiceContext();
        final Data dataBeforeTxn = putInternal(mapServiceContext.toData(key, partitionStrategy), mapServiceContext.toData(value));
        if (value != null) {
            TxnValueWrapper wrapper = dataBeforeTxn == null
                    ? new TxnValueWrapper(value, TxnValueWrapper.Type.NEW)
                    : new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED);
            txMap.put(key, wrapper);
        }
    }

    public Object putIfAbsent(Object key, Object value) {
        checkTransactionState();
        TxnValueWrapper wrapper = txMap.get(key);
        boolean haveTxnPast = wrapper != null;
        MapService service = getService();
        final MapServiceContext mapServiceContext = service.getMapServiceContext();
        if (haveTxnPast) {
            if (wrapper.type != TxnValueWrapper.Type.REMOVED) {
                return wrapper.value;
            }
            putInternal(mapServiceContext.toData(key, partitionStrategy), mapServiceContext.toData(value));
            txMap.put(key, new TxnValueWrapper(value, TxnValueWrapper.Type.NEW));
            return null;
        } else {
            Data oldValue
                    = putIfAbsentInternal(mapServiceContext.toData(key, partitionStrategy),
                    mapServiceContext.toData(value));
            if (oldValue == null) {
                txMap.put(key, new TxnValueWrapper(value, TxnValueWrapper.Type.NEW));
            }
            return mapServiceContext.toObject(oldValue);
        }
    }

    public Object replace(Object key, Object value) {
        checkTransactionState();
        TxnValueWrapper wrapper = txMap.get(key);
        boolean haveTxnPast = wrapper != null;

        MapService service = getService();
        final MapServiceContext mapServiceContext = service.getMapServiceContext();
        if (haveTxnPast) {
            if (wrapper.type == TxnValueWrapper.Type.REMOVED) {
                return null;
            }
            putInternal(mapServiceContext.toData(key, partitionStrategy), mapServiceContext.toData(value));
            txMap.put(key, new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED));
            return wrapper.value;
        } else {
            Data oldValue = replaceInternal(mapServiceContext.toData(key, partitionStrategy), mapServiceContext.toData(value));
            if (oldValue != null) {
                txMap.put(key, new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED));
            }
            return mapServiceContext.toObject(oldValue);
        }
    }

    public boolean replace(Object key, Object oldValue, Object newValue) {
        checkTransactionState();
        TxnValueWrapper wrapper = txMap.get(key);
        boolean haveTxnPast = wrapper != null;

        MapService service = getService();
        final MapServiceContext mapServiceContext = service.getMapServiceContext();
        if (haveTxnPast) {
            if (!wrapper.value.equals(oldValue)) {
                return false;
            }
            putInternal(mapServiceContext.toData(key, partitionStrategy), mapServiceContext.toData(newValue));
            txMap.put(key, new TxnValueWrapper(wrapper.value, TxnValueWrapper.Type.UPDATED));
            return true;
        } else {
            boolean success = replaceIfSameInternal(mapServiceContext.toData(key),
                    mapServiceContext.toData(oldValue), mapServiceContext.toData(newValue));
            if (success) {
                txMap.put(key, new TxnValueWrapper(newValue, TxnValueWrapper.Type.UPDATED));
            }
            return success;
        }
    }

    public boolean remove(Object key, Object value) {
        checkTransactionState();
        TxnValueWrapper wrapper = txMap.get(key);

        MapService service = getService();
        final MapServiceContext mapServiceContext = service.getMapServiceContext();
        if (wrapper != null && !mapServiceContext.compare(name, wrapper.value, value)) {
            return false;
        }
        boolean removed = removeIfSameInternal(mapServiceContext.toData(key, partitionStrategy), value);
        if (removed) {
            txMap.put(key, new TxnValueWrapper(value, TxnValueWrapper.Type.REMOVED));
        }
        return removed;
    }

    public Object remove(Object key) {
        checkTransactionState();
        MapService service = getService();
        final MapServiceContext mapServiceContext = service.getMapServiceContext();
        final Object valueBeforeTxn
                = mapServiceContext.toObject(removeInternal(mapServiceContext.toData(key, partitionStrategy)));
        TxnValueWrapper wrapper = null;
        if (valueBeforeTxn != null || txMap.containsKey(key)) {
            wrapper = txMap.put(key, new TxnValueWrapper(valueBeforeTxn, TxnValueWrapper.Type.REMOVED));
        }
        return wrapper == null ? valueBeforeTxn : checkIfRemoved(wrapper);
    }

    public void delete(Object key) {
        checkTransactionState();
        MapService service = getService();
        final MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data data = removeInternal(mapServiceContext.toData(key, partitionStrategy));
        if (data != null || txMap.containsKey(key)) {
            txMap.put(key, new TxnValueWrapper(mapServiceContext.toObject(data), TxnValueWrapper.Type.REMOVED));
        }
    }

    public Set<Object> keySet() {
        checkTransactionState();
        final Set<Data> keySet = keySetInternal();
        final Set<Object> keys = new HashSet<Object>(keySet.size());
        final MapService service = getService();
        final MapServiceContext mapServiceContext = service.getMapServiceContext();
        // convert Data to Object
        for (final Data data : keySet) {
            keys.add(mapServiceContext.toObject(data));
        }

        for (final Map.Entry<Object, TxnValueWrapper> entry : txMap.entrySet()) {
            if (TxnValueWrapper.Type.NEW.equals(entry.getValue().type)) {
                keys.add(entry.getKey());
            } else if (TxnValueWrapper.Type.REMOVED.equals(entry.getValue().type)) {
                keys.remove(entry.getKey());
            }
        }
        return keys;
    }

    public Set keySet(Predicate predicate) {
        checkTransactionState();
        if (predicate == null) {
            throw new NullPointerException("Predicate should not be null!");
        }
        if (predicate instanceof PagingPredicate) {
            throw new NullPointerException("Paging is not supported for Transactional queries!");
        }
        final MapService service = getService();
        final MapServiceContext mapServiceContext = service.getMapServiceContext();
        final QueryResultSet queryResultSet = (QueryResultSet) queryInternal(predicate, IterationType.KEY, false);
        //todo: Can't we just use the original set?
        final Set<Object> keySet = new HashSet<Object>(queryResultSet);

        for (final Map.Entry<Object, TxnValueWrapper> entry : txMap.entrySet()) {
            if (!TxnValueWrapper.Type.REMOVED.equals(entry.getValue().type)) {
                final Object value = entry.getValue().value instanceof Data
                        ? mapServiceContext.toObject(entry.getValue().value) : entry.getValue().value;

                final SerializationService ss = getNodeEngine().getSerializationService();
                final QueryEntry queryEntry =
                        new QueryEntry(ss, mapServiceContext.toData(entry.getKey()), entry.getKey(), value);
                // apply predicate on txMap.
                if (predicate.apply(queryEntry)) {
                    keySet.add(entry.getKey());
                }
            } else {
                // meanwhile remove keys which are not in txMap.
                keySet.remove(entry.getKey());
            }
        }
        return keySet;
    }

    public Collection<Object> values() {
        checkTransactionState();
        final Collection<Data> dataSet = valuesInternal();
        final Collection<Object> values = new ArrayList<Object>(dataSet.size());
        final MapService service = getService();
        final MapServiceContext mapServiceContext = service.getMapServiceContext();
        for (final Data data : dataSet) {
            values.add(mapServiceContext.toObject(data));
        }
        for (TxnValueWrapper wrapper : txMap.values()) {
            if (TxnValueWrapper.Type.NEW.equals(wrapper.type)) {
                values.add(wrapper.value);
            } else if (TxnValueWrapper.Type.REMOVED.equals(wrapper.type)) {
                values.remove(wrapper.value);
            }
        }
        return values;
    }

    public Collection values(Predicate predicate) {
        checkTransactionState();
        if (predicate == null) {
            throw new NullPointerException("Predicate can not be null!");
        }
        if (predicate instanceof PagingPredicate) {
            throw new IllegalArgumentException("Paging is not supported for Transactional queries");
        }
        final MapService service = getService();
        final MapServiceContext mapServiceContext = service.getMapServiceContext();
        final QueryResultSet queryResultSet = (QueryResultSet) queryInternal(predicate, IterationType.ENTRY, false);
        //todo: Can't we just use the original set?
        final Set<Object> valueSet = new HashSet<Object>();
        final Set<Object> keyWontBeIncluded = new HashSet<Object>();

        // iterate over the txMap and see if the values are updated or removed.
        for (final Map.Entry<Object, TxnValueWrapper> entry : txMap.entrySet()) {
            final boolean isRemoved = TxnValueWrapper.Type.REMOVED.equals(entry.getValue().type);
            final boolean isUpdated = TxnValueWrapper.Type.UPDATED.equals(entry.getValue().type);

            Object objectKey = entry.getKey();
            if (isRemoved) {
                keyWontBeIncluded.add(objectKey);
            } else {
                if (isUpdated) {
                    keyWontBeIncluded.add(objectKey);
                }
                Object entryValue = entry.getValue().value;
                final Object objectValue = entryValue instanceof Data
                        ? mapServiceContext.toObject(entryValue) : entryValue;
                Data dataKey = mapServiceContext.toData(objectKey);
                final SerializationService serializationService = getNodeEngine().getSerializationService();
                final QueryEntry queryEntry = new QueryEntry(serializationService, dataKey, objectKey, objectValue);
                if (predicate.apply(queryEntry)) {
                    valueSet.add(entryValue);
                }
            }
        }
        removeFromResultSet(queryResultSet, valueSet, keyWontBeIncluded);
        return valueSet;
    }

    private void removeFromResultSet(QueryResultSet queryResultSet, Set<Object> valueSet, Set<Object> keyWontBeIncluded) {
        final Iterator<Map.Entry> iterator = queryResultSet.rawIterator();
        while (iterator.hasNext()) {
            final Map.Entry entry = iterator.next();
            if (keyWontBeIncluded.contains(entry.getKey())) {
                continue;
            }
            valueSet.add(entry.getValue());
        }
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("TransactionalMap");
        sb.append("{name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }

}
