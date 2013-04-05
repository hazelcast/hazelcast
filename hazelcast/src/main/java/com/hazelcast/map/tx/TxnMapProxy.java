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
import com.hazelcast.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

/**
 * @mdogan 2/26/13
 */
public class TxnMapProxy extends TxnMapProxySupport implements TransactionalMap {

    private static final Object NULL = new Object();

    private final Map<Object, Object> txMap = new HashMap<Object, Object>();

    public TxnMapProxy(String name, MapService mapService, NodeEngine nodeEngine, Transaction transaction) {
        super(name, mapService, nodeEngine, transaction);
    }

    public boolean containsKey(Object key) {
        return txMap.containsKey(key) || containsKeyInternal(getService().toData(key));
    }

    public Object get(Object key) {
        Object currentValue = txMap.get(key);
        if (currentValue != null) {
            return checkIfNull(currentValue);
        }
        final Object value = getService().toObject(getInternal(getService().toData(key)));
        return checkIfNull(value);
    }

    private Object checkIfNull(Object value) {
        return value == null || value.equals(NULL) ? null : value;
    }

    public Object put(Object key, Object value) {
        final Object valueBeforeTxn = getService().toObject(putInternal(getService().toData(key), getService().toData(value)));
        Object currentValue = txMap.get(key);
        if (value != null)
            txMap.put(key, value);
        return currentValue == null ? valueBeforeTxn : checkIfNull(currentValue);
    }

    @Override
    public void set(Object key, Object value) {
        if (value != null)
            txMap.put(key, value);
        setInternal(getService().toData(key), getService().toData(value));
    }

    @Override
    public Object putIfAbsent(Object key, Object value) {
        Object current = txMap.get(key);
        boolean haveTxnPast = current != null;
        if (haveTxnPast) {
            if (!current.equals(NULL)) {
                return current;
            }
            setInternal(getService().toData(key), getService().toData(value));
            txMap.put(key, value);
            return null;
        } else {
            Data oldValue = putIfAbsentInternal(getService().toData(key), getService().toData(value));
            if (oldValue == null)
                txMap.put(key, value);
            return getService().toObject(oldValue);
        }
    }

    @Override
    public Object replace(Object key, Object value) {
        Object current = txMap.get(key);
        boolean haveTxnPast = current != null;

        if(haveTxnPast) {
            if(current.equals(NULL)){
                return null;
            }
            setInternal(getService().toData(key), getService().toData(value));
            txMap.put(key, value);
            return current;
        }
        else {
            Data oldValue = replaceInternal(getService().toData(key), getService().toData(value));
            if(oldValue != null)
                txMap.put(key, value);
            return getService().toObject(oldValue);
        }
    }

    @Override
    public boolean replace(Object key, Object oldValue, Object newValue) {
        Object current = txMap.get(key);
        boolean haveTxnPast = current != null;

        if(haveTxnPast) {
            if(!current.equals(oldValue)){
                return false;
            }
            setInternal(getService().toData(key), getService().toData(newValue));
            txMap.put(key, newValue);
            return true;
        }
        else {
            boolean success = replaceIfSameInternal(getService().toData(key), getService().toData(oldValue), getService().toData(newValue));
            if(success)
                txMap.put(key, newValue);
            return success;
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        Object current = txMap.get(key);
        if (current != null && !getService().compare(name, current, value)) {
            return false;
        }
        boolean removed = removeIfSameInternal(getService().toData(key), value);
        if (removed) {
            txMap.put(key, NULL);
        }
        return removed;
    }

    @Override
    public Object remove(Object key) {
        final Object valueBeforeTxn = getService().toObject(removeInternal(getService().toData(key)));
        Object currentValue = txMap.put(key, NULL);
        return currentValue == null ? valueBeforeTxn : checkIfNull(currentValue);
    }

    @Override
    public void delete(Object key) {
        removeInternal(getService().toData(key));
        txMap.put(key, NULL);
    }

}
