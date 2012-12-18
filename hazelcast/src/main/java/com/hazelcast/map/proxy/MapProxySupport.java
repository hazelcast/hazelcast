/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.proxy;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.MapEntry;
import com.hazelcast.core.Transaction;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.map.*;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.Data;
import com.hazelcast.query.Expression;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.TransactionImpl;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.map.MapService.MAP_SERVICE_NAME;

abstract class MapProxySupport {

    protected final String name;
    protected final MapService mapService;
    protected final NodeEngine nodeEngine;

    protected MapProxySupport(final String name, final MapService mapService, NodeEngine nodeEngine) {
        this.name = name;
        this.mapService = mapService;
        this.nodeEngine = nodeEngine;
    }

    protected Data getInternal(Data key) {
        int partitionId = nodeEngine.getPartitionId(key);
        GetOperation operation = new GetOperation(name, key);
        operation.setThreadId(ThreadContext.get().getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future f = invocation.invoke();
            return (Data) f.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Future<Data> getAsyncInternal(final Data key) {
        return null;
    }

    protected Data putInternal(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        int partitionId = nodeEngine.getPartitionId(key);
        String txnId = prepareTransaction(partitionId);
        PutOperation operation = new PutOperation(name, key, value, txnId, getTimeInMillis(ttl, timeunit));
        operation.setThreadId(ThreadContext.get().getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future f = invocation.invoke();
            return (Data) f.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected boolean tryPutInternal(final Data key, final Data value, final long timeout, final TimeUnit timeunit) {
        int partitionId = nodeEngine.getPartitionId(key);
        String txnId = prepareTransaction(partitionId);
        TryPutOperation operation = new TryPutOperation(name, key, value, txnId, getTimeInMillis(timeout, timeunit));
        operation.setThreadId(ThreadContext.get().getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future f = invocation.invoke();
            return (Boolean) f.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Data putIfAbsentInternal(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        int partitionId = nodeEngine.getPartitionId(key);
        String txnId = prepareTransaction(partitionId);
        PutIfAbsentOperation operation = new PutIfAbsentOperation(name, key, value, txnId, getTimeInMillis(ttl, timeunit));
        operation.setThreadId(ThreadContext.get().getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future f = invocation.invoke();
            return (Data) f.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected void putTransientInternal(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        int partitionId = nodeEngine.getPartitionId(key);
        String txnId = prepareTransaction(partitionId);
        PutTransientOperation operation = new PutTransientOperation(name, key, value, txnId, getTimeInMillis(ttl, timeunit));
        operation.setThreadId(ThreadContext.get().getThreadId());
        operation.setServiceName(MAP_SERVICE_NAME);
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            invocation.invoke();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Future<Data> putAsyncInternal(final Data key, final Data value) {
        return null;
    }

    protected boolean replaceInternal(final Data key, final Data oldValue, final Data newValue) {
        int partitionId = nodeEngine.getPartitionId(key);
        String txnId = prepareTransaction(partitionId);
        ReplaceIfSameOperation operation = new ReplaceIfSameOperation(name, key, oldValue, newValue, txnId);
        operation.setThreadId(ThreadContext.get().getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future f = invocation.invoke();
            return (Boolean) f.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Data replaceInternal(final Data key, final Data value) {
        int partitionId = nodeEngine.getPartitionId(key);
        String txnId = prepareTransaction(partitionId);
        ReplaceOperation operation = new ReplaceOperation(name, key, value, txnId);
        operation.setThreadId(ThreadContext.get().getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future f = invocation.invoke();
            return (Data) f.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected void setInternal(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        int partitionId = nodeEngine.getPartitionId(key);
        String txnId = prepareTransaction(partitionId);
        SetOperation setOperation = new SetOperation(name, key, value, txnId, ttl);
        setOperation.setThreadId(ThreadContext.get().getThreadId());
        setOperation.setServiceName(MAP_SERVICE_NAME);
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, setOperation, partitionId)
                    .build();
            invocation.invoke();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Data removeInternal(Data key) {
        int partitionId = nodeEngine.getPartitionId(key);
        TransactionImpl txn = nodeEngine.getTransaction();
        String txnId = prepareTransaction(partitionId);
        RemoveOperation operation = new RemoveOperation(name, key, txnId);
        operation.setThreadId(ThreadContext.get().getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future f = invocation.invoke();
            return (Data) f.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected boolean removeInternal(final Data key, final Data value) {
        return false;
    }

    protected Object tryRemoveInternal(final Data key, final long timeout, final TimeUnit timeunit) throws TimeoutException {
        return null;
    }

    protected Future<Data> removeAsyncInternal(final Data key) {
        return null;
    }

    protected boolean containsKeyInternal(Data key) {
        int partitionId = nodeEngine.getPartitionId(key);
        ContainsKeyOperation containsKeyOperation = new ContainsKeyOperation(name, key);
        containsKeyOperation.setServiceName(MAP_SERVICE_NAME);
        containsKeyOperation.setThreadId(ThreadContext.get().getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, containsKeyOperation,
                    partitionId).build();
            Future f = invocation.invoke();
            return (Boolean) nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    protected boolean containsValueInternal(final Data value) {
        return false;
    }

    public int size() {
        try {
            MapSizeOperation mapSizeOperation = new MapSizeOperation(name);
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(MAP_SERVICE_NAME, mapSizeOperation);
            int total = 0;
            for (Object result : results.values()) {
                Integer size = (Integer) nodeEngine.toObject(result);
                total += size;
            }
            return total;
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    public boolean isEmpty() {
        try {
            MapIsEmptyOperation mapIsEmptyOperation = new MapIsEmptyOperation(name);
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(MAP_SERVICE_NAME, mapIsEmptyOperation);
            for (Object result : results.values()) {
                if (!(Boolean) nodeEngine.toObject(result))
                    return false;
            }
            return true;
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Map<Data, Data> getAllInternal(final Set<Data> keys) {
        return null;
    }

    protected void putAllInternal(final Map<? extends Data, ? extends Data> m) {
    }

    protected void lockInternal(final Data key) {
        int partitionId = nodeEngine.getPartitionId(key);
        LockOperation operation = new LockOperation(name, key);
        operation.setThreadId(ThreadContext.get().getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future future = invocation.invoke();
            future.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected void unlockInternal(final Data key) {
        int partitionId = nodeEngine.getPartitionId(key);
        UnlockOperation operation = new UnlockOperation(name, key);
        operation.setThreadId(ThreadContext.get().getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future future = invocation.invoke();
            future.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected boolean isLockedInternal(final Data key) {
        return false;
    }

    protected boolean tryLockInternal(final Data key, final long time, final TimeUnit timeunit) {
        return false;
    }

    protected void forceUnlockInternal(final Data key) {

    }

    public boolean lockMap(final long time, final TimeUnit timeunit) {
        return false;
    }

    public void unlockMap() {
    }

    protected void addLocalEntryListenerInternal(final EntryListener<Data, Data> listener) {
    }

    protected void addEntryListenerInternal(final EntryListener<Data, Data> listener, final boolean includeValue) {
    }

    protected void removeEntryListenerInternal(final EntryListener<Data, Data> listener) {
    }

    protected void addEntryListenerInternal(final EntryListener<Data, Data> listener, final Data key, final boolean includeValue) {
    }

    protected void removeEntryListenerInternal(final EntryListener<Data, Data> listener, final Data key) {
    }

    protected MapEntry<Data, Data> getMapEntryInternal(final Data key) {
        return null;
    }

    protected boolean evictInternal(final Data key) {
        return false;
    }

    public void clear() {
    }

    public void flush() {
    }

    protected Set<Data> keySetInternal() {
        return null;
    }

    protected Collection<Data> valuesInternal() {
        return null;
    }

    protected Set<Entry<Data, Data>> entrySetInternal() {
        return null;
    }

    protected Set<Data> keySetInternal(final Predicate predicate) {
        return null;
    }

    protected Set<Entry<Data, Data>> entrySetInternal(final Predicate predicate) {
        return null;
    }

    protected Collection<Data> valuesInternal(final Predicate predicate) {
        return null;
    }

    protected Set<Data> localKeySetInternal() {
        return null;
    }

    protected Set<Data> localKeySetInternal(final Predicate predicate) {
        return null;
    }

    public void addIndex(final String attribute, final boolean ordered) {

    }

    public void addIndex(final Expression<?> expression, final boolean ordered) {

    }

    public LocalMapStats getLocalMapStats() {
        return null;
    }

    public void destroy() {
    }

    protected String prepareTransaction(int partitionId) {
        TransactionImpl txn = nodeEngine.getTransaction();
        String txnId = null;
        if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
            txnId = txn.getTxnId();
            txn.attachParticipant(MAP_SERVICE_NAME, partitionId);
        }
        return txnId;
    }

    protected long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }
}
