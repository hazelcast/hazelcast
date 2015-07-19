/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.QueryResultSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotInstanceOf;
import static com.hazelcast.util.Preconditions.checkNotNull;

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
        Data keyData = getService().getMapServiceContext().toData(key, partitionStrategy);

        TxnValueWrapper valueWrapper = txMap.get(keyData);
        if (valueWrapper != null) {
            return (valueWrapper.type != TxnValueWrapper.Type.REMOVED);
        }
        return containsKeyInternal(keyData);
    }

    @Override
    public int size() {
        checkTransactionState();
        int currentSize = sizeInternal();
        for (Map.Entry<Data, TxnValueWrapper> entry : txMap.entrySet()) {
            TxnValueWrapper wrapper = entry.getValue();
            if (wrapper.type == TxnValueWrapper.Type.NEW) {
                currentSize++;
            } else if (wrapper.type == TxnValueWrapper.Type.REMOVED) {
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
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        TxnValueWrapper currentValue = txMap.get(keyData);
        if (currentValue != null) {
            return checkIfRemoved(currentValue);
        }
        return mapServiceContext.toObject(getInternal(keyData));
    }

    @Override
    public Object getForUpdate(Object key) {
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        TxnValueWrapper currentValue = txMap.get(keyData);
        if (currentValue != null) {
            return checkIfRemoved(currentValue);
        }

        return mapServiceContext.toObject(getForUpdateInternal(keyData));
    }

    @Override
    public Object put(Object key, Object value) {
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        Object valueBeforeTxn = mapServiceContext.toObject(putInternal(keyData, mapServiceContext.toData(value)));

        TxnValueWrapper currentValue = txMap.get(keyData);
        if (value != null) {
            TxnValueWrapper wrapper = valueBeforeTxn == null
                    ? new TxnValueWrapper(value, TxnValueWrapper.Type.NEW)
                    : new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED);

            txMap.put(keyData, wrapper);
        }
        return currentValue == null ? valueBeforeTxn : checkIfRemoved(currentValue);
    }

    @Override
    public Object put(Object key, Object value, long ttl, TimeUnit timeUnit) {
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        Object valueBeforeTxn = mapServiceContext.toObject(putInternal(keyData, mapServiceContext.toData(value), ttl, timeUnit));

        TxnValueWrapper currentValue = txMap.get(keyData);
        if (value != null) {
            TxnValueWrapper wrapper = valueBeforeTxn == null
                    ? new TxnValueWrapper(value, TxnValueWrapper.Type.NEW)
                    : new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED);
            txMap.put(keyData, wrapper);
        }
        return currentValue == null ? valueBeforeTxn : checkIfRemoved(currentValue);
    }

    @Override
    public void set(Object key, Object value) {
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        Data dataBeforeTxn = putInternal(keyData, mapServiceContext.toData(value));
        if (value != null) {
            TxnValueWrapper wrapper = (dataBeforeTxn == null)
                    ? new TxnValueWrapper(value, TxnValueWrapper.Type.NEW)
                    : new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED);
            txMap.put(keyData, wrapper);
        }
    }

    @Override
    public Object putIfAbsent(Object key, Object value) {
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        TxnValueWrapper wrapper = txMap.get(keyData);
        boolean haveTxnPast = wrapper != null;
        if (haveTxnPast) {
            if (wrapper.type != TxnValueWrapper.Type.REMOVED) {
                return wrapper.value;
            }
            putInternal(keyData, mapServiceContext.toData(value));
            txMap.put(keyData, new TxnValueWrapper(value, TxnValueWrapper.Type.NEW));
            return null;
        } else {
            Data oldValue
                    = putIfAbsentInternal(keyData,
                    mapServiceContext.toData(value));
            if (oldValue == null) {
                txMap.put(keyData, new TxnValueWrapper(value, TxnValueWrapper.Type.NEW));
            }
            return mapServiceContext.toObject(oldValue);
        }
    }

    @Override
    public Object replace(Object key, Object value) {
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        TxnValueWrapper wrapper = txMap.get(keyData);
        boolean haveTxnPast = wrapper != null;
        if (haveTxnPast) {
            if (wrapper.type == TxnValueWrapper.Type.REMOVED) {
                return null;
            }
            putInternal(keyData, mapServiceContext.toData(value));
            txMap.put(keyData, new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED));
            return wrapper.value;
        } else {
            Data oldValue = replaceInternal(keyData, mapServiceContext.toData(value));
            if (oldValue != null) {
                txMap.put(keyData, new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED));
            }
            return mapServiceContext.toObject(oldValue);
        }
    }

    @Override
    public boolean replace(Object key, Object oldValue, Object newValue) {
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);

        TxnValueWrapper wrapper = txMap.get(keyData);
        boolean haveTxnPast = wrapper != null;
        if (haveTxnPast) {
            if (!wrapper.value.equals(oldValue)) {
                return false;
            }
            putInternal(keyData, mapServiceContext.toData(newValue));
            txMap.put(keyData, new TxnValueWrapper(wrapper.value, TxnValueWrapper.Type.UPDATED));
            return true;
        } else {
            boolean success = replaceIfSameInternal(keyData,
                    mapServiceContext.toData(oldValue), mapServiceContext.toData(newValue));
            if (success) {
                txMap.put(keyData, new TxnValueWrapper(newValue, TxnValueWrapper.Type.UPDATED));
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

        TxnValueWrapper wrapper = txMap.get(keyData);
        if (wrapper != null && !mapServiceContext.compare(name, wrapper.value, value)) {
            return false;
        }

        boolean removed = removeIfSameInternal(keyData, value);
        if (removed) {
            txMap.put(keyData, new TxnValueWrapper(value, TxnValueWrapper.Type.REMOVED));
        }
        return removed;
    }

    @Override
    public Object remove(Object key) {
        checkTransactionState();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Data keyData = mapServiceContext.toData(key, partitionStrategy);
        Object valueBeforeTxn = mapServiceContext.toObject(removeInternal(keyData));

        TxnValueWrapper wrapper = null;
        if (valueBeforeTxn != null || txMap.containsKey(keyData)) {
            wrapper = txMap.put(keyData, new TxnValueWrapper(valueBeforeTxn, TxnValueWrapper.Type.REMOVED));
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
            txMap.put(keyData, new TxnValueWrapper(mapServiceContext.toObject(data), TxnValueWrapper.Type.REMOVED));
        }
    }

    @Override
    public Set<Object> keySet() {
        checkTransactionState();
        Set<Data> keySet = keySetInternal();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();

        for (Map.Entry<Data, TxnValueWrapper> entry : txMap.entrySet()) {
            if (TxnValueWrapper.Type.NEW.equals(entry.getValue().type)) {
                keySet.add(entry.getKey());
            } else if (TxnValueWrapper.Type.REMOVED.equals(entry.getValue().type)) {
                keySet.remove(entry.getKey());
            }
        }
        HashSet<Object> keys = new HashSet<Object>();
        for (Data keyData : keySet) {
            keys.add(mapServiceContext.toObject(keyData));
        }
        return keys;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set keySet(Predicate predicate) {
        checkTransactionState();
        checkNotNull(predicate, "Predicate should not be null!");
        checkNotInstanceOf(PagingPredicate.class, predicate, "Paging is not supported for Transactional queries!");

        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        SerializationService ss = getNodeEngine().getSerializationService();

        QueryResultSet queryResultSet = (QueryResultSet) queryInternal(predicate, IterationType.KEY, false);
        // TODO: Can't we just use the original set?
        Set<Object> keySet = new HashSet<Object>(queryResultSet);
        for (Map.Entry<Data, TxnValueWrapper> entry : txMap.entrySet()) {
            Object key = ss.toObject(entry.getKey());
            if (!TxnValueWrapper.Type.REMOVED.equals(entry.getValue().type)) {
                Object value = entry.getValue().value instanceof Data
                        ? mapServiceContext.toObject(entry.getValue().value) : entry.getValue().value;

                QueryEntry queryEntry = new QueryEntry(ss, entry.getKey(), key, value);
                // apply predicate on txMap
                if (predicate.apply(queryEntry)) {
                    keySet.add(key);
                }
            } else {
                // meanwhile remove keys which are not in txMap
                keySet.remove(key);
            }
        }
        return keySet;
    }

    @Override
    public Collection<Object> values() {
        checkTransactionState();
        List<Map.Entry<Data, Data>> entries = getEntries();
        MapService service = getService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        Collection<Object> values = new ArrayList<Object>(entries.size());
        Set<Data> keyWontBeIncluded = new HashSet<Data>();

        for (Map.Entry<Data, TxnValueWrapper> entry : txMap.entrySet()) {
            boolean isRemoved = TxnValueWrapper.Type.REMOVED.equals(entry.getValue().type);
            boolean isUpdated = TxnValueWrapper.Type.UPDATED.equals(entry.getValue().type);

            if (isRemoved) {
                keyWontBeIncluded.add(entry.getKey());
            } else {
                if (isUpdated) {
                    keyWontBeIncluded.add(entry.getKey());
                }
                Object entryValue = entry.getValue().value;
                values.add(entryValue);
            }
        }
        for (Map.Entry<Data, Data> entry : entries) {
            if (keyWontBeIncluded.contains(entry.getKey())) {
                continue;
            }
            Object value = mapServiceContext.toObject(entry.getValue());
            values.add(value);
        }
        return values;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection values(Predicate predicate) {
        checkTransactionState();
        checkNotNull(predicate, "Predicate can not be null!");
        checkNotInstanceOf(PagingPredicate.class, predicate, "Paging is not supported for Transactional queries");

        SerializationService serializationService = getNodeEngine().getSerializationService();

        QueryResultSet queryResultSet = (QueryResultSet) queryInternal(predicate, IterationType.ENTRY, false);
        // TODO: Can't we just use the original set?
        Set<Object> valueSet = new HashSet<Object>();
        Set<Object> keyWontBeIncluded = new HashSet<Object>();

        // iterate over the txMap and see if the values are updated or removed
        for (Map.Entry<Data, TxnValueWrapper> entry : txMap.entrySet()) {
            boolean isRemoved = TxnValueWrapper.Type.REMOVED.equals(entry.getValue().type);
            boolean isUpdated = TxnValueWrapper.Type.UPDATED.equals(entry.getValue().type);

            Object keyObject = serializationService.toObject(entry.getKey());
            if (isRemoved) {
                keyWontBeIncluded.add(keyObject);
            } else {
                if (isUpdated) {
                    keyWontBeIncluded.add(keyObject);
                }
                Object entryValue = entry.getValue().value;

                QueryEntry queryEntry = new QueryEntry(serializationService, entry.getKey(), keyObject, entryValue);
                if (predicate.apply(queryEntry)) {
                    valueSet.add(queryEntry.getValue());
                }
            }
        }
        removeFromResultSet(queryResultSet, valueSet, keyWontBeIncluded);
        return valueSet;
    }

    @Override
    public String toString() {
        return "TransactionalMap" + "{name='" + name + '\'' + '}';
    }

    private Object checkIfRemoved(TxnValueWrapper wrapper) {
        checkTransactionState();
        return wrapper == null || wrapper.type == TxnValueWrapper.Type.REMOVED ? null : wrapper.value;
    }

    private void removeFromResultSet(QueryResultSet queryResultSet, Set<Object> valueSet, Set<Object> keyWontBeIncluded) {
        Iterator<Map.Entry> iterator = queryResultSet.rawIterator();
        while (iterator.hasNext()) {
            Map.Entry entry = iterator.next();
            if (keyWontBeIncluded.contains(entry.getKey())) {
                continue;
            }
            valueSet.add(entry.getValue());
        }
    }
}
