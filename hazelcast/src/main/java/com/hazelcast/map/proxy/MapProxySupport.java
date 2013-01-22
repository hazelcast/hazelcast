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

package com.hazelcast.map.proxy;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.core.Transaction;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.map.*;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Expression;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.transaction.TransactionImpl;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.map.MapService.MAP_SERVICE_NAME;

abstract class MapProxySupport extends AbstractDistributedObject {

    protected final String name;
    protected final MapService mapService;

    protected MapProxySupport(final String name, final MapService mapService, NodeEngine nodeEngine) {
        super(nodeEngine);
        this.name = name;
        this.mapService = mapService;
    }

    protected Data getInternal(Data key) {
        int partitionId = nodeEngine.getPartitionId(key);
        GetOperation operation = new GetOperation(name, key);
        operation.setThreadId(ThreadContext.getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future invoke = invocation.invoke();
            return (Data) invoke.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Future<Data> getAsyncInternal(final Data key) {
        int partitionId = nodeEngine.getPartitionId(key);
        GetOperation operation = new GetOperation(name, key);
        operation.setThreadId(ThreadContext.getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            return nodeEngine.getAsyncInvocationService().invoke(invocation);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Data putInternal(final Data key, final Data value, final long ttl, final TimeUnit timeunit) {
        int partitionId = nodeEngine.getPartitionId(key);
        String txnId = prepareTransaction(partitionId);
        PutOperation operation = new PutOperation(name, key, value, txnId, getTimeInMillis(ttl, timeunit));
        operation.setThreadId(ThreadContext.getThreadId());
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
        operation.setThreadId(ThreadContext.getThreadId());
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
        operation.setThreadId(ThreadContext.getThreadId());
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
        operation.setThreadId(ThreadContext.getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future f = invocation.invoke();
            f.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Future<Data> putAsyncInternal(final Data key, final Data value) {
        int partitionId = nodeEngine.getPartitionId(key);
        String txnId = prepareTransaction(partitionId);
        PutOperation operation = new PutOperation(name, key, value, txnId, -1);
        operation.setThreadId(ThreadContext.getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            return nodeEngine.getAsyncInvocationService().invoke(invocation);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected boolean replaceInternal(final Data key, final Data oldValue, final Data newValue) {
        int partitionId = nodeEngine.getPartitionId(key);
        String txnId = prepareTransaction(partitionId);
        ReplaceIfSameOperation operation = new ReplaceIfSameOperation(name, key, oldValue, newValue, txnId);
        operation.setThreadId(ThreadContext.getThreadId());
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
        operation.setThreadId(ThreadContext.getThreadId());
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
        setOperation.setThreadId(ThreadContext.getThreadId());
        setOperation.setServiceName(MAP_SERVICE_NAME);
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, setOperation, partitionId)
                    .build();
            invocation.invoke();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected boolean evictInternal(final Data key) {
        int partitionId = nodeEngine.getPartitionId(key);
        String txnId = prepareTransaction(partitionId);
        EvictOperation operation = new EvictOperation(name, key, txnId);
        operation.setThreadId(ThreadContext.getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future f = invocation.invoke();
            return (Boolean) f.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Data removeInternal(Data key) {
        int partitionId = nodeEngine.getPartitionId(key);
        String txnId = prepareTransaction(partitionId);
        RemoveOperation operation = new RemoveOperation(name, key, txnId);
        operation.setThreadId(ThreadContext.getThreadId());
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
        int partitionId = nodeEngine.getPartitionId(key);
        String txnId = prepareTransaction(partitionId);
        RemoveIfSameOperation operation = new RemoveIfSameOperation(name, key, value, txnId);
        operation.setThreadId(ThreadContext.getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future f = invocation.invoke();
            return (Boolean) f.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Data tryRemoveInternal(final Data key, final long timeout, final TimeUnit timeunit) throws TimeoutException {
        int partitionId = nodeEngine.getPartitionId(key);
        String txnId = prepareTransaction(partitionId);
        TryRemoveOperation operation = new TryRemoveOperation(name, key, txnId, getTimeInMillis(timeout, timeunit));
        operation.setThreadId(ThreadContext.getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future f = invocation.invoke();
            return (Data) f.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Future<Data> removeAsyncInternal(final Data key) {
        int partitionId = nodeEngine.getPartitionId(key);
        String txnId = prepareTransaction(partitionId);
        RemoveOperation operation = new RemoveOperation(name, key, txnId);
        operation.setThreadId(ThreadContext.getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            return nodeEngine.getAsyncInvocationService().invoke(invocation);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected boolean containsKeyInternal(Data key) {
        int partitionId = nodeEngine.getPartitionId(key);
        ContainsKeyOperation containsKeyOperation = new ContainsKeyOperation(name, key);
        containsKeyOperation.setServiceName(MAP_SERVICE_NAME);
        containsKeyOperation.setThreadId(ThreadContext.getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, containsKeyOperation,
                    partitionId).build();
            Future f = invocation.invoke();
            return (Boolean) nodeEngine.toObject(f.get());
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
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

    public boolean containsValueInternal(Data dataValue) {
        try {
            ContainsValueOperation containsValueOperation = new ContainsValueOperation(name, dataValue);
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(MAP_SERVICE_NAME, containsValueOperation);

            for (Object result : results.values()) {
                Boolean contains = (Boolean) nodeEngine.toObject(result);
                if (contains)
                    return true;
            }
            return false;
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

    // todo optimize this
    protected Map<Data, Data> getAllDataInternal(final Set<Data> keys) {
        Map<Data, Data> res = new HashMap(keys.size());
        for (Data key : keys) {
            res.put(key, getInternal(key));
        }
        return res;
    }

    // todo optimize this
    protected Map<Object, Object> getAllObjectInternal(final Set<Data> keys) {
        Map<Object, Object> res = new HashMap(keys.size());
        for (Data key : keys) {
            res.put(nodeEngine.toObject(key), nodeEngine.toObject(getInternal(key)));
        }
        return res;
    }

    // todo optimize these
    protected void putAllDataInternal(final Map<? extends Data, ? extends Data> m) {
        for (Entry<? extends Data, ? extends Data> entry : m.entrySet()) {
            putInternal(entry.getKey(), entry.getValue(), -1, null);
        }
    }

    // todo optimize these
    protected void putAllObjectInternal(final Map<? extends Object, ? extends Object> m) {
        for (Entry<? extends Object, ? extends Object> entry : m.entrySet()) {
            putInternal(nodeEngine.toData(entry.getKey()), nodeEngine.toData(entry.getValue()), -1, null);
        }
    }

    protected void lockInternal(final Data key) {
        int partitionId = nodeEngine.getPartitionId(key);
        LockOperation operation = new LockOperation(name, key);
        operation.setThreadId(ThreadContext.getThreadId());
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
        operation.setThreadId(ThreadContext.getThreadId());
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
        int partitionId = nodeEngine.getPartitionId(key);
        IsLockedOperation operation = new IsLockedOperation(name, key);
        operation.setThreadId(ThreadContext.getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future future = invocation.invoke();
            return (Boolean) future.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected boolean tryLockInternal(final Data key, final long timeout, final TimeUnit timeunit) {
        int partitionId = nodeEngine.getPartitionId(key);
        TryLockOperation operation = new TryLockOperation(name, key, getTimeInMillis(timeout, timeunit));
        operation.setThreadId(ThreadContext.getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future future = invocation.invoke();
            return (Boolean) future.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Set<Data> keySetInternal() {
        try {
            MapKeySetOperation mapKeySetOperation = new MapKeySetOperation(name);
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(MAP_SERVICE_NAME, mapKeySetOperation);
            Set<Data> keySet = new HashSet<Data>();
            for (Object result : results.values()) {
                Set keys = ((MapKeySet) nodeEngine.toObject(result)).getKeySet();
                keySet.addAll(keys);
            }
            return keySet;
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Set<Data> localKeySetInternal() {
        try {
            MapKeySetOperation mapKeySetOperation = new MapKeySetOperation(name);
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnTargetPartitions(MAP_SERVICE_NAME, mapKeySetOperation, nodeEngine.getThisAddress());
            Set<Data> keySet = new HashSet<Data>();
            for (Object result : results.values()) {
                Set keys = ((MapKeySet) nodeEngine.toObject(result)).getKeySet();
                keySet.addAll(keys);
            }
            return keySet;
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    public void flushInternal(boolean flushAll) {
        try {
            MapFlushOperation mapFlushOperation = new MapFlushOperation(name, flushAll);
            nodeEngine.getOperationService()
                    .invokeOnAllPartitions(MAP_SERVICE_NAME, mapFlushOperation);
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    protected Collection<Data> valuesInternal() {
        try {
            MapValuesOperation mapValuesOperation = new MapValuesOperation(name);
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(MAP_SERVICE_NAME, mapValuesOperation);
            List<Data> values = new ArrayList<Data>();
            for (Object result : results.values()) {
                values.addAll(((MapValueCollection) nodeEngine.toObject(result)).getValues());
            }
            return values;
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    // todo certainly must be optimized
    public void clearInternal() {
        Set<Data> keys = keySetInternal();
        for (Data key : keys) {
            removeInternal(key);
        }
    }

    protected void forceUnlockInternal(final Data key) {
        int partitionId = nodeEngine.getPartitionId(key);
        ForceUnlockOperation operation = new ForceUnlockOperation(name, key);
        operation.setThreadId(ThreadContext.getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future future = invocation.invoke();
            future.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    public void addMapInterceptorInternal(MapInterceptor interceptor) {
        String id = mapService.addInterceptor(name, interceptor);
        AddInterceptorOperation operation = new AddInterceptorOperation(id, interceptor, name);
        Set<Member> members = nodeEngine.getCluster().getMembers();
        for (Member member : members) {
            try {
                if (member.localMember())
                    continue;
                MemberImpl memberImpl = (MemberImpl) member;
                Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, memberImpl.getAddress()).build();
                invocation.invoke().get();
            } catch (Throwable throwable) {
                throw new HazelcastException(throwable);
            }
        }
    }

    public void removeMapInterceptorInternal(MapInterceptor interceptor) {
        String id = mapService.removeInterceptor(name, interceptor);
        RemoveInterceptorOperation operation = new RemoveInterceptorOperation(interceptor, name, id);
        Set<Member> members = nodeEngine.getCluster().getMembers();
        for (Member member : members) {
            try {
                if (member.localMember())
                    continue;
                MemberImpl memberImpl = (MemberImpl) member;
                Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, memberImpl.getAddress()).build();
                invocation.invoke().get();
            } catch (Throwable throwable) {
                throw new HazelcastException(throwable);
            }
        }
    }

    protected void addLocalEntryListenerInternal(final EntryListener<Data, Data> listener) {
    }

    protected void removeEntryListenerInternal(final EntryListener listener) {
        removeEntryListenerInternal(listener, null);
    }

    protected void addEntryListenerInternal(final EntryListener listener, final Data key, final boolean includeValue) {
        EventFilter eventFilter = new EntryEventFilter(includeValue, key);
        mapService.addEventListener(listener, eventFilter, name);
    }

    protected void addEntryListenerInternal(EntryListener listener, Predicate predicate, final Data key, final boolean includeValue) {
        EventFilter eventFilter = new QueryEventFilter(includeValue, key, predicate);
        mapService.addEventListener(listener, eventFilter, name);
    }

    protected void removeEntryListenerInternal(final EntryListener listener, final Data key) {
        mapService.removeEventListener(listener, name, key);
    }

    protected Map.Entry<Data, Data> getMapEntryInternal(final Data key) {
        int partitionId = nodeEngine.getPartitionId(key);
        GetMapEntryOperation getMapEntryOperation = new GetMapEntryOperation(name, key);
        getMapEntryOperation.setServiceName(MAP_SERVICE_NAME);
        getMapEntryOperation.setThreadId(ThreadContext.getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, getMapEntryOperation,
                    partitionId).build();
            Future f = invocation.invoke();
            Object o = nodeEngine.toObject(f.get());
            return (Map.Entry<Data, Data>) o;
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    protected Set<Entry<Data, Data>> entrySetInternal() {
        try {
            MapEntrySetOperation mapEntrySetOperation = new MapEntrySetOperation(name);
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(MAP_SERVICE_NAME, mapEntrySetOperation);
            Set<Entry<Data, Data>> entrySet = new HashSet<Entry<Data, Data>>();
            for (Object result : results.values()) {
                Set entries = ((MapEntrySet) nodeEngine.toObject(result)).getEntrySet();
                if (entries != null)
                    entrySet.addAll(entries);
            }
            return entrySet;
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    public Data executeOnKeyInternal(Data key, EntryProcessor entryProcessor) {
        int partitionId = nodeEngine.getPartitionId(key);
        EntryOperation operation = new EntryOperation(name, key, entryProcessor);
        operation.setThreadId(ThreadContext.getThreadId());
        try {
            Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(MAP_SERVICE_NAME, operation, partitionId)
                    .build();
            Future future = invocation.invoke();
            return (Data) future.get();
        } catch (Throwable throwable) {
            throw new HazelcastException(throwable);
        }
    }

    public void flush() {
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

    public final Object getId() {
        return name;
    }

    public final String getName() {
        return name;
    }

    public final String getServiceName() {
        return MAP_SERVICE_NAME;
    }
}
