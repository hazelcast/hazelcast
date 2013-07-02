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
import com.hazelcast.map.MapRecordKey;
import com.hazelcast.map.MapService;
import com.hazelcast.map.operation.ClearOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.BinaryOperationFactory;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.util.ExceptionUtil;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.map.MapService.SERVICE_NAME;

/**
 * @author mdogan 2/26/13
 */
public class TxnMapProxy extends TxnMapProxySupport implements TransactionalMap {

    private final Map<Object, TxnValueWrapper> txMap = new HashMap<Object, TxnValueWrapper>();

    public TxnMapProxy(String name, MapService mapService, NodeEngine nodeEngine, Transaction transaction) {
        super(name, mapService, nodeEngine, transaction);
    }

    public boolean containsKey(Object key) {
        return txMap.containsKey(key) || containsKeyInternal(getService().toData(key));
    }

    public int size() {
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
        return size() == 0;
    }

    public Object get(Object key) {
        TxnValueWrapper currentValue = txMap.get(key);
        if (currentValue != null) {
            return checkIfRemoved(currentValue);
        }
        return getService().toObject(getInternal(getService().toData(key)));
    }

    private Object checkIfRemoved(TxnValueWrapper wrapper) {
        return wrapper == null || wrapper.type == TxnValueWrapper.Type.REMOVED ? null : wrapper.value;
    }

    public Object put(Object key, Object value) {
        final Object valueBeforeTxn = getService().toObject(putInternal(getService().toData(key), getService().toData(value)));
        TxnValueWrapper currentValue = txMap.get(key);
        if (value != null) {
            TxnValueWrapper wrapper = valueBeforeTxn == null ? new TxnValueWrapper(value, TxnValueWrapper.Type.NEW) : new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED);
            txMap.put(key, wrapper);
        }
        return currentValue == null ? valueBeforeTxn : checkIfRemoved(currentValue);
    }

    @Override
    public void set(Object key, Object value) {
        final Data dataBeforeTxn = putInternal(getService().toData(key), getService().toData(value));
        if (value != null) {
            TxnValueWrapper wrapper = dataBeforeTxn == null ? new TxnValueWrapper(value, TxnValueWrapper.Type.NEW) : new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED);
            txMap.put(key, wrapper);
        }
    }

    @Override
    public Object putIfAbsent(Object key, Object value) {
        TxnValueWrapper wrapper = txMap.get(key);
        boolean haveTxnPast = wrapper != null;
        if (haveTxnPast) {
            if (wrapper.type != TxnValueWrapper.Type.REMOVED) {
                return wrapper.value;
            }
            putInternal(getService().toData(key), getService().toData(value));
            txMap.put(key, new TxnValueWrapper(value, TxnValueWrapper.Type.NEW));
            return null;
        } else {
            Data oldValue = putIfAbsentInternal(getService().toData(key), getService().toData(value));
            if (oldValue == null) {
                txMap.put(key, new TxnValueWrapper(value, TxnValueWrapper.Type.NEW));
            }
            return getService().toObject(oldValue);
        }
    }

    @Override
    public Object replace(Object key, Object value) {
        TxnValueWrapper wrapper = txMap.get(key);
        boolean haveTxnPast = wrapper != null;

        if (haveTxnPast) {
            if (wrapper.type == TxnValueWrapper.Type.REMOVED) {
                return null;
            }
            putInternal(getService().toData(key), getService().toData(value));
            txMap.put(key, new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED));
            return wrapper.value;
        } else {
            Data oldValue = replaceInternal(getService().toData(key), getService().toData(value));
            if (oldValue != null) {
                txMap.put(key, new TxnValueWrapper(value, TxnValueWrapper.Type.UPDATED));
            }
            return getService().toObject(oldValue);
        }
    }

    @Override
    public boolean replace(Object key, Object oldValue, Object newValue) {
        TxnValueWrapper wrapper = txMap.get(key);
        boolean haveTxnPast = wrapper != null;

        if (haveTxnPast) {
            if (!wrapper.value.equals(oldValue)) {
                return false;
            }
            putInternal(getService().toData(key), getService().toData(newValue));
            txMap.put(key, new TxnValueWrapper(wrapper.value, TxnValueWrapper.Type.UPDATED));
            return true;
        } else {
            boolean success = replaceIfSameInternal(getService().toData(key), getService().toData(oldValue), getService().toData(newValue));
            if (success) {
                txMap.put(key, new TxnValueWrapper(newValue, TxnValueWrapper.Type.UPDATED));
            }
            return success;
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        TxnValueWrapper wrapper = txMap.get(key);

        if (wrapper != null && !getService().compare(name, wrapper.value, value)) {
            return false;
        }
        boolean removed = removeIfSameInternal(getService().toData(key), value);
        if (removed) {
            txMap.put(key, new TxnValueWrapper(value, TxnValueWrapper.Type.REMOVED));
        }
        return removed;
    }

    @Override
    public Object remove(Object key) {
        final Object valueBeforeTxn = getService().toObject(removeInternal(getService().toData(key)));
        TxnValueWrapper wrapper = null;
        if(valueBeforeTxn != null || txMap.containsKey(key) ) {
            wrapper = txMap.put(key, new TxnValueWrapper(valueBeforeTxn, TxnValueWrapper.Type.REMOVED));
        }
        return wrapper == null ? valueBeforeTxn : checkIfRemoved(wrapper);
    }

    @Override
    public void delete(Object key) {
        Data data = removeInternal(getService().toData(key));
        if(data != null || txMap.containsKey(key) ) {
            txMap.put(key, new TxnValueWrapper(getService().toObject(data), TxnValueWrapper.Type.REMOVED));
        }
    }

}
