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
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.impl.TransactionSupport;

import java.util.HashMap;
import java.util.Map;

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

    private Object checkIfRemoved(TxnValueWrapper wrapper) {
        checkTransactionState();
        return wrapper == null || wrapper.type == TxnValueWrapper.Type.REMOVED ? null : wrapper.value;
    }

    public Object put(Object key, Object value) {
        checkTransactionState();
        MapService service = getService();
        final Object valueBeforeTxn = service.toObject(putInternal(service.toData(key, partitionStrategy), service.toData(value)));
        TxnValueWrapper currentValue = txMap.get(key);
        if (value != null) {
            TxnValueWrapper wrapper = valueBeforeTxn == null ? new TxnValueWrapper(value, TxnValueWrapper.Type.NEW) : new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED);
            txMap.put(key, wrapper);
        }
        return currentValue == null ? valueBeforeTxn : checkIfRemoved(currentValue);
    }

    @Override
    public void set(Object key, Object value) {
        checkTransactionState();
        MapService service = getService();
        final Data dataBeforeTxn = putInternal(service.toData(key, partitionStrategy), service.toData(value));
        if (value != null) {
            TxnValueWrapper wrapper = dataBeforeTxn == null ? new TxnValueWrapper(value, TxnValueWrapper.Type.NEW) : new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED);
            txMap.put(key, wrapper);
        }
    }

    @Override
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

    @Override
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

    @Override
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

    @Override
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

    @Override
    public Object remove(Object key) {
        checkTransactionState();
        MapService service = getService();
        final Object valueBeforeTxn = service.toObject(removeInternal(service.toData(key, partitionStrategy)));
        TxnValueWrapper wrapper = null;
        if(valueBeforeTxn != null || txMap.containsKey(key) ) {
            wrapper = txMap.put(key, new TxnValueWrapper(valueBeforeTxn, TxnValueWrapper.Type.REMOVED));
        }
        return wrapper == null ? valueBeforeTxn : checkIfRemoved(wrapper);
    }

    @Override
    public void delete(Object key) {
        checkTransactionState();
        MapService service = getService();
        Data data = removeInternal(service.toData(key, partitionStrategy));
        if(data != null || txMap.containsKey(key) ) {
            txMap.put(key, new TxnValueWrapper(service.toObject(data), TxnValueWrapper.Type.REMOVED));
        }
    }

}
