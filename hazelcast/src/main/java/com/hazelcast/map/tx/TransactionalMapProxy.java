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

package com.hazelcast.map.tx;

import com.hazelcast.core.TransactionalMap;
import com.hazelcast.map.MapService;
import com.hazelcast.nio.serialization.Data;
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
 * @author mdogan 2/26/13
 */
public class TransactionalMapProxy extends TransactionalMapProxySupport implements TransactionalMap {

    private final Map<Object, TxnValueWrapper> txMap = new HashMap<Object, TxnValueWrapper>();

    public TransactionalMapProxy(String name, MapService mapService, NodeEngine nodeEngine, TransactionSupport transaction) {
        super(name, mapService, nodeEngine, transaction);
    }

    public boolean containsKey(Object key) {
        checkTransactionState();
        return txMap.containsKey(key) || containsKeyInternal(getService().toData(key, partitionStrategy));
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
        return getService().toObject(getInternal(getService().toData(key, partitionStrategy)));
    }

    public Object getForUpdate(Object key) {
        checkTransactionState();
        TxnValueWrapper currentValue = txMap.get(key);
        if (currentValue != null) {
            return checkIfRemoved(currentValue);
        }
        Data dataKey = getService().toData(key, partitionStrategy);
        return getService().toObject(getForUpdateInternal(dataKey));
    }

    private Object checkIfRemoved(TxnValueWrapper wrapper) {
        checkTransactionState();
        return wrapper == null || wrapper.type == TxnValueWrapper.Type.REMOVED ? null : wrapper.value;
    }

    public Object put(Object key, Object value) {
        checkTransactionState();
        MapService service = getService();
        final Object valueBeforeTxn = service.toObject(putInternal(service.toData(key, partitionStrategy),
                service.toData(value)));
        TxnValueWrapper currentValue = txMap.get(key);
        if (value != null) {
            TxnValueWrapper wrapper = valueBeforeTxn == null ?
                    new TxnValueWrapper(value, TxnValueWrapper.Type.NEW) :
                    new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED);
            txMap.put(key, wrapper);
        }
        return currentValue == null ? valueBeforeTxn : checkIfRemoved(currentValue);
    }

    public Object put(Object key, Object value, long ttl, TimeUnit timeUnit) {
        checkTransactionState();
        MapService service = getService();
        final Object valueBeforeTxn = service.toObject(putInternal(service.toData(key, partitionStrategy),
                service.toData(value), ttl, timeUnit));
        TxnValueWrapper currentValue = txMap.get(key);
        if (value != null) {
            TxnValueWrapper wrapper = valueBeforeTxn == null ?
                    new TxnValueWrapper(value, TxnValueWrapper.Type.NEW) :
                    new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED);
            txMap.put(key, wrapper);
        }
        return currentValue == null ? valueBeforeTxn : checkIfRemoved(currentValue);
    }

    public void set(Object key, Object value) {
        checkTransactionState();
        MapService service = getService();
        final Data dataBeforeTxn = putInternal(service.toData(key, partitionStrategy), service.toData(value));
        if (value != null) {
            TxnValueWrapper wrapper = dataBeforeTxn == null ? new TxnValueWrapper(value, TxnValueWrapper.Type.NEW) : new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED);
            txMap.put(key, wrapper);
        }
    }

    public Object putIfAbsent(Object key, Object value) {
        checkTransactionState();
        TxnValueWrapper wrapper = txMap.get(key);
        boolean haveTxnPast = wrapper != null;
        MapService service = getService();
        if (haveTxnPast) {
            if (wrapper.type != TxnValueWrapper.Type.REMOVED) {
                return wrapper.value;
            }
            putInternal(service.toData(key, partitionStrategy), service.toData(value));
            txMap.put(key, new TxnValueWrapper(value, TxnValueWrapper.Type.NEW));
            return null;
        } else {
            Data oldValue = putIfAbsentInternal(service.toData(key, partitionStrategy), service.toData(value));
            if (oldValue == null) {
                txMap.put(key, new TxnValueWrapper(value, TxnValueWrapper.Type.NEW));
            }
            return service.toObject(oldValue);
        }
    }

    public Object replace(Object key, Object value) {
        checkTransactionState();
        TxnValueWrapper wrapper = txMap.get(key);
        boolean haveTxnPast = wrapper != null;

        MapService service = getService();
        if (haveTxnPast) {
            if (wrapper.type == TxnValueWrapper.Type.REMOVED) {
                return null;
            }
            putInternal(service.toData(key, partitionStrategy), service.toData(value));
            txMap.put(key, new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED));
            return wrapper.value;
        } else {
            Data oldValue = replaceInternal(service.toData(key, partitionStrategy), service.toData(value));
            if (oldValue != null) {
                txMap.put(key, new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED));
            }
            return service.toObject(oldValue);
        }
    }

    public boolean replace(Object key, Object oldValue, Object newValue) {
        checkTransactionState();
        TxnValueWrapper wrapper = txMap.get(key);
        boolean haveTxnPast = wrapper != null;

        MapService service = getService();
        if (haveTxnPast) {
            if (!wrapper.value.equals(oldValue)) {
                return false;
            }
            putInternal(service.toData(key, partitionStrategy), service.toData(newValue));
            txMap.put(key, new TxnValueWrapper(wrapper.value, TxnValueWrapper.Type.UPDATED));
            return true;
        } else {
            boolean success = replaceIfSameInternal(service.toData(key), service.toData(oldValue), service.toData(newValue));
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
        if (wrapper != null && !service.compare(name, wrapper.value, value)) {
            return false;
        }
        boolean removed = removeIfSameInternal(service.toData(key, partitionStrategy), value);
        if (removed) {
            txMap.put(key, new TxnValueWrapper(value, TxnValueWrapper.Type.REMOVED));
        }
        return removed;
    }

    public Object remove(Object key) {
        checkTransactionState();
        MapService service = getService();
        final Object valueBeforeTxn = service.toObject(removeInternal(service.toData(key, partitionStrategy)));
        TxnValueWrapper wrapper = null;
        if (valueBeforeTxn != null || txMap.containsKey(key)) {
            wrapper = txMap.put(key, new TxnValueWrapper(valueBeforeTxn, TxnValueWrapper.Type.REMOVED));
        }
        return wrapper == null ? valueBeforeTxn : checkIfRemoved(wrapper);
    }

    public void delete(Object key) {
        checkTransactionState();
        MapService service = getService();
        Data data = removeInternal(service.toData(key, partitionStrategy));
        if (data != null || txMap.containsKey(key)) {
            txMap.put(key, new TxnValueWrapper(service.toObject(data), TxnValueWrapper.Type.REMOVED));
        }
    }

    public Set<Object> keySet() {
        checkTransactionState();
        final Set<Data> keySet = keySetInternal();
        final Set<Object> keys = new HashSet<Object>(keySet.size());
        final MapService service = getService();
        // convert Data to Object
        for (final Data data : keySet) {
            keys.add(service.toObject(data));
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
        final QueryResultSet queryResultSet = (QueryResultSet) queryInternal(predicate, IterationType.KEY, false);
        final Set<Object> keySet = new HashSet<Object>(queryResultSet); //todo: Can't we just use the original set?

        for (final Map.Entry<Object, TxnValueWrapper> entry : txMap.entrySet()) {
            if (!TxnValueWrapper.Type.REMOVED.equals(entry.getValue().type)) {
                final Object value = entry.getValue().value instanceof Data ?
                        service.toObject(entry.getValue().value) : entry.getValue().value;
                final QueryEntry queryEntry = new QueryEntry(null, service.toData(entry.getKey()), entry.getKey(), value);
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
        for (final Data data : dataSet) {
            values.add(getService().toObject(data));
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
        final QueryResultSet queryResultSet = (QueryResultSet) queryInternal(predicate, IterationType.ENTRY, false);
        final Set<Object> valueSet = new HashSet<Object>(); //todo: Can't we just use the original set?
        final Set<Object> keyWontBeIncluded = new HashSet<Object>();

        // delete updated or removed elements from the result set
        for (final Map.Entry<Object, TxnValueWrapper> entry : txMap.entrySet()) {
            final boolean isRemoved = TxnValueWrapper.Type.REMOVED.equals(entry.getValue().type);
            final boolean isUpdated = TxnValueWrapper.Type.UPDATED.equals(entry.getValue().type);

            if (isRemoved) {
                keyWontBeIncluded.add(entry.getKey());
            } else {
                if (isUpdated) {
                    keyWontBeIncluded.add(entry.getKey());
                }
                final Object entryValue = entry.getValue().value;
                final Object objectValue = entryValue instanceof Data ?
                        service.toObject(entryValue) : entryValue;
                final QueryEntry queryEntry = new QueryEntry(null, service.toData(entry.getKey()), entry.getKey(), objectValue);
                // apply predicate on txMap.
                if (predicate.apply(queryEntry)) {
                    valueSet.add(entryValue);
                }
            }
        }

        final Iterator<Map.Entry> iterator = queryResultSet.rawIterator();
        while (iterator.hasNext()) {
            final Map.Entry entry = iterator.next();
            if (keyWontBeIncluded.contains(entry.getKey())) {
                continue;
            }
            valueSet.add(entry.getValue());
        }
        return valueSet;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("TransactionalMap");
        sb.append("{name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }

}
