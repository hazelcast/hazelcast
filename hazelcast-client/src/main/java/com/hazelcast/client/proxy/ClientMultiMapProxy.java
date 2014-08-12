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

package com.hazelcast.client.proxy;

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.MappingJob;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.mapreduce.ReducingSubmittableJob;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.monitor.LocalMultiMapStats;
import com.hazelcast.multimap.impl.operations.client.AddEntryListenerRequest;
import com.hazelcast.multimap.impl.operations.client.ClearRequest;
import com.hazelcast.multimap.impl.operations.client.ContainsValueRequest;
import com.hazelcast.multimap.impl.operations.client.CountRequest;
import com.hazelcast.multimap.impl.operations.client.EntrySetRequest;
import com.hazelcast.multimap.impl.operations.client.GetAllRequest;
import com.hazelcast.multimap.impl.operations.client.KeySetRequest;
import com.hazelcast.multimap.impl.operations.client.MultiMapIsLockedRequest;
import com.hazelcast.multimap.impl.operations.client.MultiMapLockRequest;
import com.hazelcast.multimap.impl.operations.client.MultiMapUnlockRequest;
import com.hazelcast.multimap.impl.operations.client.PortableEntrySetResponse;
import com.hazelcast.multimap.impl.operations.client.PutRequest;
import com.hazelcast.multimap.impl.operations.client.RemoveAllRequest;
import com.hazelcast.multimap.impl.operations.client.RemoveEntryListenerRequest;
import com.hazelcast.multimap.impl.operations.client.RemoveRequest;
import com.hazelcast.multimap.impl.operations.client.SizeRequest;
import com.hazelcast.multimap.impl.operations.client.ValuesRequest;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.PortableCollection;
import com.hazelcast.spi.impl.PortableEntryEvent;
import com.hazelcast.util.ThreadUtil;
import com.hazelcast.util.ValidationUtil;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ValidationUtil.isNotNull;
import static com.hazelcast.util.ValidationUtil.shouldBePositive;

/**
 * @author ali 5/19/13
 */
public class ClientMultiMapProxy<K, V> extends ClientProxy implements MultiMap<K, V> {

    private final String name;

    public ClientMultiMapProxy(String instanceName, String serviceName, String name) {
        super(instanceName, serviceName, name);
        this.name = name;
    }

    public boolean put(K key, V value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        PutRequest request = new PutRequest(name, keyData, valueData, -1, ThreadUtil.getThreadId());
        Boolean result = invoke(request, keyData);
        return result;
    }

    public Collection<V> get(K key) {
        Data keyData = toData(key);
        GetAllRequest request = new GetAllRequest(name, keyData);
        PortableCollection result = invoke(request, keyData);
        return toObjectCollection(result, true);
    }

    public boolean remove(Object key, Object value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        RemoveRequest request = new RemoveRequest(name, keyData, valueData, ThreadUtil.getThreadId());
        Boolean result = invoke(request, keyData);
        return result;
    }

    public Collection<V> remove(Object key) {
        Data keyData = toData(key);
        RemoveAllRequest request = new RemoveAllRequest(name, keyData, ThreadUtil.getThreadId());
        PortableCollection result = invoke(request, keyData);
        return toObjectCollection(result, true);
    }

    public Set<K> localKeySet() {
        throw new UnsupportedOperationException("Locality for client is ambiguous");
    }

    public Set<K> keySet() {
        KeySetRequest request = new KeySetRequest(name);
        PortableCollection result = invoke(request);
        return (Set) toObjectCollection(result, false);
    }

    public Collection<V> values() {
        ValuesRequest request = new ValuesRequest(name);
        PortableCollection result = invoke(request);
        return toObjectCollection(result, true);
    }

    public Set<Map.Entry<K, V>> entrySet() {
        EntrySetRequest request = new EntrySetRequest(name);
        PortableEntrySetResponse result = invoke(request);
        Set<Map.Entry> dataEntrySet = result.getEntrySet();
        Set<Map.Entry<K, V>> entrySet = new HashSet<Map.Entry<K, V>>(dataEntrySet.size());
        for (Map.Entry entry : dataEntrySet) {
            Object key = toObject((Data) entry.getKey());
            Object val = toObject((Data) entry.getValue());
            entrySet.add(new AbstractMap.SimpleEntry(key, val));
        }
        return entrySet;
    }

    public boolean containsKey(K key) {
        Data keyData = toData(key);
        ContainsValueRequest request = new ContainsValueRequest(name, keyData, null);
        Boolean result = invoke(request, keyData);
        return result;
    }

    public boolean containsValue(Object value) {
        Data valueData = toData(value);
        ContainsValueRequest request = new ContainsValueRequest(name, null, valueData);
        Boolean result = invoke(request);
        return result;
    }

    public boolean containsEntry(K key, V value) {
        Data keyData = toData(key);
        Data valueData = toData(value);
        ContainsValueRequest request = new ContainsValueRequest(name, keyData, valueData);
        Boolean result = invoke(request, keyData);
        return result;
    }

    public int size() {
        SizeRequest request = new SizeRequest(name);
        Integer result = invoke(request);
        return result;
    }

    public void clear() {
        ClearRequest request = new ClearRequest(name);
        invoke(request);
    }

    public int valueCount(K key) {
        Data keyData = toData(key);
        CountRequest request = new CountRequest(name, keyData);
        Integer result = invoke(request, keyData);
        return result;
    }

    public String addLocalEntryListener(EntryListener<K, V> listener) {
        throw new UnsupportedOperationException("Locality for client is ambiguous");
    }

    public String addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
        isNotNull(listener, "listener");
        AddEntryListenerRequest request = new AddEntryListenerRequest(name, null, includeValue);
        EventHandler<PortableEntryEvent> handler = createHandler(listener, includeValue);
        return listen(request, handler);
    }

    public boolean removeEntryListener(String registrationId) {
        final RemoveEntryListenerRequest request = new RemoveEntryListenerRequest(name, registrationId);
        return stopListening(request, registrationId);
    }

    public String addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {
        final Data keyData = toData(key);
        AddEntryListenerRequest request = new AddEntryListenerRequest(name, keyData, includeValue);
        EventHandler<PortableEntryEvent> handler = createHandler(listener, includeValue);
        return listen(request, keyData, handler);
    }

    public void lock(K key) {
        final Data keyData = toData(key);
        MultiMapLockRequest request = new MultiMapLockRequest(keyData, ThreadUtil.getThreadId(), name);
        invoke(request, keyData);
    }

    public void lock(K key, long leaseTime, TimeUnit timeUnit) {
        shouldBePositive(leaseTime, "leaseTime");
        final Data keyData = toData(key);
        MultiMapLockRequest request = new MultiMapLockRequest(keyData, ThreadUtil.getThreadId(),
                getTimeInMillis(leaseTime, timeUnit), -1, name);
        invoke(request, keyData);
    }

    public boolean isLocked(K key) {
        final Data keyData = toData(key);
        final MultiMapIsLockedRequest request = new MultiMapIsLockedRequest(keyData, name);
        final Boolean result = invoke(request, keyData);
        return result;
    }

    public boolean tryLock(K key) {
        try {
            return tryLock(key, 0, null);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public boolean tryLock(K key, long time, TimeUnit timeunit) throws InterruptedException {
        final Data keyData = toData(key);
        MultiMapLockRequest request = new MultiMapLockRequest(keyData, ThreadUtil.getThreadId(),
                Long.MAX_VALUE, getTimeInMillis(time, timeunit), name);
        Boolean result = invoke(request, keyData);
        return result;
    }

    public void unlock(K key) {
        final Data keyData = toData(key);
        MultiMapUnlockRequest request = new MultiMapUnlockRequest(keyData, ThreadUtil.getThreadId(), name);
        invoke(request, keyData);
    }

    public void forceUnlock(K key) {
        final Data keyData = toData(key);
        MultiMapUnlockRequest request = new MultiMapUnlockRequest(keyData, ThreadUtil.getThreadId(), true, name);
        invoke(request, keyData);
    }

    public LocalMultiMapStats getLocalMultiMapStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!!!");
    }

    @Override
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation) {

        HazelcastInstance hazelcastInstance = getContext().getHazelcastInstance();
        JobTracker jobTracker = hazelcastInstance.getJobTracker("hz::aggregation-multimap-" + getName());
        return aggregate(supplier, aggregation, jobTracker);
    }

    @Override
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation,
                                                    JobTracker jobTracker) {

        try {
            ValidationUtil.isNotNull(jobTracker, "jobTracker");
            KeyValueSource<K, V> keyValueSource = KeyValueSource.fromMultiMap(this);
            Job<K, V> job = jobTracker.newJob(keyValueSource);
            Mapper mapper = aggregation.getMapper(supplier);
            CombinerFactory combinerFactory = aggregation.getCombinerFactory();
            ReducerFactory reducerFactory = aggregation.getReducerFactory();
            Collator collator = aggregation.getCollator();

            MappingJob mappingJob = job.mapper(mapper);
            ReducingSubmittableJob reducingJob;
            if (combinerFactory != null) {
                reducingJob = mappingJob.combiner(combinerFactory).reducer(reducerFactory);
            } else {
                reducingJob = mappingJob.reducer(reducerFactory);
            }

            ICompletableFuture<Result> future = reducingJob.submit(collator);
            return future.get();
        } catch (Exception e) {
            throw new HazelcastException(e);
        }
    }

    protected void onDestroy() {
    }

    private Collection toObjectCollection(PortableCollection result, boolean list) {
        Collection<Data> coll = result.getCollection();
        Collection collection;
        if (list) {
            collection = new ArrayList(coll == null ? 0 : coll.size());
        } else {
            collection = new HashSet(coll == null ? 0 : coll.size());
        }
        if (coll == null) {
            return collection;
        }
        for (Data data : coll) {
            collection.add(toObject(data));
        }
        return collection;
    }

    protected long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    private EventHandler<PortableEntryEvent> createHandler(final EntryListener<K, V> listener, final boolean includeValue) {
        return new EventHandler<PortableEntryEvent>() {
            public void handle(PortableEntryEvent event) {
                Member member = getContext().getClusterService().getMember(event.getUuid());
                switch (event.getEventType()) {
                    case ADDED:
                        listener.entryAdded(createEntryEvent(event, member));
                        break;
                    case REMOVED:
                        listener.entryRemoved(createEntryEvent(event, member));
                        break;
                    case CLEAR_ALL:
                        listener.mapCleared(createMapEvent(event, member));
                        break;
                    default:
                        throw new IllegalArgumentException("Not a known event type " + event.getEventType());
                }
            }

            private MapEvent createMapEvent(PortableEntryEvent event, Member member) {
                return new MapEvent(name, member, event.getEventType().getType(), event.getNumberOfAffectedEntries());
            }

            private EntryEvent<K, V> createEntryEvent(PortableEntryEvent event, Member member) {
                V value = null;
                V oldValue = null;
                if (includeValue) {
                    value = toObject(event.getValue());
                    oldValue = toObject(event.getOldValue());
                }
                K key = toObject(event.getKey());
                return new EntryEvent<K, V>(name, member,
                        event.getEventType().getType(), key, oldValue, value);
            }

            @Override
            public void onListenerRegister() {
            }
        };
    }

    @Override
    public String toString() {
        return "MultiMap{" + "name='" + getName() + '\'' + '}';
    }
}
