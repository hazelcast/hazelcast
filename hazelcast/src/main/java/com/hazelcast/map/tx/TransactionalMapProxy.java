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
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryResultEntry;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.QueryResultSet;

import java.util.*;

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

    @Override
    public Set<Object> keySet() {
        checkTransactionState();
        final Set<Data> keySet = keySetInternal();
        final Set<Object> keys = new HashSet<Object>( keySet.size() );
        final MapService service = getService();
        // convert Data to Object
        for ( final Data data: keySet )
        {
            keys.add( service.toObject( data ) );
        }

        for ( final Map.Entry<Object, TxnValueWrapper> entry : txMap.entrySet() )
        {
            if( TxnValueWrapper.Type.NEW.equals( entry.getValue().type ) )
            {
                keys.add( entry.getKey() );
            }
            else if( TxnValueWrapper.Type.REMOVED.equals( entry.getValue().type ) )
            {
                keys.remove( entry.getKey() );
            }
        }
        return keys;
    }

    @Override
    public Set keySet(Predicate predicate) {
        checkTransactionState();
        if ( predicate == null) {
            throw new NullPointerException("Predicate should not be null!");
        }

        final MapService service = getService();
        final QueryResultSet queryResultSet = (QueryResultSet) queryInternal(predicate, IterationType.KEY, false);
        final Set<Object> keySet = new HashSet<Object>( queryResultSet.size() );
        while( queryResultSet.iterator().hasNext() )
        {
            keySet.add(service.toObject(((QueryResultEntry) queryResultSet.iterator().next()).getKeyData()));
        }

        for ( final Map.Entry<Object, TxnValueWrapper> entry : txMap.entrySet() )
        {
            if( !TxnValueWrapper.Type.REMOVED.equals( entry.getValue().type ) )
            {
                // apply predicate on txMap.
                if( predicate.apply( new QueryEntry( null, service.toData(entry.getKey()),
                        entry.getKey(), entry.getValue().value ) )) {
                    keySet.add(entry.getKey());
                }
            }
            else
            {
                // meanwhile remove keys which are not in txMap.
                keySet.remove(entry.getKey());
            }
        }

        return  keySet;
    }

    @Override
    public Collection<Object> values() {
        checkTransactionState();
        final Collection<Data> dataSet = valuesInternal();
        final Collection<Object> values = new ArrayList<Object>( dataSet.size() );
        for (final Data data : dataSet) {
            values.add(getService().toObject(data));
        }
        for (TxnValueWrapper wrapper : txMap.values()) {
            if( TxnValueWrapper.Type.NEW.equals( wrapper.type ) )
            {
                values.add(wrapper.value);
            }
            else if( TxnValueWrapper.Type.REMOVED.equals( wrapper.type ) )
            {
                values.remove(wrapper.value);
            }
        }
        return values;
    }

    @Override
    public Collection values(Predicate predicate) {
        checkTransactionState();
        if ( predicate == null) {
            throw new NullPointerException("Predicate can not be null!");
        }

        final MapService service = getService();
        final QueryResultSet queryResultSet = (QueryResultSet) queryInternal(predicate, IterationType.VALUE, false);
        final Set<Object> valueSet = new HashSet<Object>( queryResultSet.size() );

        final Iterator iterator = queryResultSet.iterator();
        while( iterator.hasNext() )
        {
            valueSet.add( iterator.next() );
        }

        for ( final Map.Entry<Object, TxnValueWrapper> entry : txMap.entrySet() )
        {
            if( !TxnValueWrapper.Type.REMOVED.equals( entry.getValue().type ) )
            {
                // apply predicate on txMap.
                if( predicate.apply( new QueryEntry( null, service.toData(entry.getKey()),
                        entry.getKey(), entry.getValue().value ) )) {
                    valueSet.add( entry.getValue().value );
                }
            }
            else
            {
                // meanwhile remove values which are not in txMap.
                valueSet.remove( entry.getValue() );
            }
        }

        return valueSet;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("TransactionalMap");
        sb.append("{name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }

}
