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

package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.*;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.*;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.mapreduce.*;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.monitor.LocalMultiMapStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.PortableCollection;
import com.hazelcast.util.ThreadUtil;
import com.hazelcast.util.ValidationUtil;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.ListenerAdapters.createListenerAdapter;
import static com.hazelcast.multimap.impl.ValueCollectionFactory.createCollection;
import static com.hazelcast.util.ValidationUtil.*;

/**
 * @author ali 5/19/13
 */
public class ClientMultiMapProxy<K, V> extends ClientProxy implements MultiMap<K, V> {

    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";

    private final String name;

    public ClientMultiMapProxy(String serviceName, String name) {
        super(serviceName, name);
        this.name = name;
    }

    public boolean put(K key, V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        Data valueData = toData(value);
        ClientMessage request = MultiMapPutParameters.encode(name, keyData, valueData, ThreadUtil.getThreadId(), -1);
        ClientMessage response = invoke(request, keyData);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public Collection<V> get(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        ClientMessage request = MultiMapGetParameters.encode(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        DataCollectionResultParameters resultParameters = DataCollectionResultParameters.decode(response);
        Collection<Data> result = resultParameters.result;
        Collection<V> resultCollection = new ArrayList<V>(result.size());
        for (Data data : result) {
            final V value = toObject(data);
            resultCollection.add(value);
        }
        return resultCollection;
    }

    public boolean remove(Object key, Object value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        Data valueData = toData(value);
        ClientMessage request = MultiMapRemoveEntryParameters.encode(name, keyData, valueData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public Collection<V> remove(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        ClientMessage request = MultiMapRemoveParameters.encode(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        DataCollectionResultParameters resultParameters = DataCollectionResultParameters.decode(response);
        Collection<Data> result = resultParameters.result;
        Collection<V> resultCollection = new ArrayList<V>(result.size());
        for (Data data : result) {
            final V value = toObject(data);
            resultCollection.add(value);
        }
        return resultCollection;
    }

    public Set<K> localKeySet() {
        throw new UnsupportedOperationException("Locality for client is ambiguous");
    }

    public Set<K> keySet() {
        ClientMessage request = MultiMapKeySetParameters.encode(name);
        ClientMessage response = invoke(request);
        DataCollectionResultParameters resultParameters = DataCollectionResultParameters.decode(response);
        Collection<Data> result = resultParameters.result;
        Set<K> keySet = new HashSet<K>(result.size());
        for (Data data : result) {
            final K key = toObject(data);
            keySet.add(key);
        }
        return keySet;
    }

    public Collection<V> values() {
        ClientMessage request = MultiMapValuesParameters.encode(name);
        ClientMessage response = invoke(request);
        DataCollectionResultParameters resultParameters = DataCollectionResultParameters.decode(response);
        Collection<Data> result = resultParameters.result;
        Collection<V> resultCollection = new ArrayList<V>(result.size());
        for (Data data : result) {
            final V value = toObject(data);
            resultCollection.add(value);
        }
        return resultCollection;
    }

    public Set<Map.Entry<K, V>> entrySet() {
        ClientMessage request = MultiMapEntrySetParameters.encode(name);
        ClientMessage response = invoke(request);
        DataEntryListResultParameters resultParameters = DataEntryListResultParameters.decode(response);
        Set<Map.Entry<K, V>> entrySet = new HashSet<Map.Entry<K, V>>();
        int size = resultParameters.keys.size();

        for (int i = 0; i < size; i++) {
            Data keyData = resultParameters.keys.get(i);
            Data valueData = resultParameters.values.get(i);
            K key = toObject(keyData);
            V value = toObject(valueData);

            entrySet.add(new AbstractMap.SimpleEntry<K, V>(key, value));
        }
        return entrySet;
    }

    public boolean containsKey(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        ClientMessage request = MultiMapContainsKeyParameters.encode(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean containsValue(Object value) {
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyValue = toData(value);
        ClientMessage request = MultiMapContainsValueParameters.encode(name, keyValue);
        ClientMessage response = invoke(request);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean containsEntry(K key, V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        Data valueData = toData(value);
        ClientMessage request = MultiMapContainsEntryParameters.encode(name, keyData, valueData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public int size() {
        ClientMessage request = MultiMapSizeParameters.encode(name);
        ClientMessage response = invoke(request);
        IntResultParameters resultParameters = IntResultParameters.decode(response);
        return resultParameters.result;
    }

    public void clear() {
        ClientMessage request = MultiMapClearParameters.encode(name);
        invoke(request);
    }

    public int valueCount(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        ClientMessage request = MultiMapValueCountParameters.encode(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        IntResultParameters resultParameters = IntResultParameters.decode(response);
        return resultParameters.result;
    }

    public String addLocalEntryListener(EntryListener<K, V> listener) {
        throw new UnsupportedOperationException("Locality for client is ambiguous");
    }

    public String addEntryListener(EntryListener<K, V> listener, boolean includeValue) {
        isNotNull(listener, "listener");
        ClientMessage request = MultiMapAddEntryListenerParameters.encode(name, includeValue);

        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        return listen(request, handler);
    }

    public boolean removeEntryListener(String registrationId) {
        ClientMessage request = MultiMapRemoveEntryListenerParameters.encode(name, registrationId);
        return stopListening(request, registrationId);
    }

    public String addEntryListener(EntryListener<K, V> listener, K key, boolean includeValue) {
        final Data keyData = toData(key);
        ClientMessage request = MultiMapAddEntryListenerToKeyParameters.encode(name, keyData, includeValue);

        EventHandler<ClientMessage> handler = createHandler(listener, includeValue);
        return listen(request, keyData, handler);
    }

    public void lock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        ClientMessage request = MultiMapLockParameters.encode(name, keyData, ThreadUtil.getThreadId());
        invoke(request, keyData);
    }

    public void lock(K key, long leaseTime, TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        shouldBePositive(leaseTime, "leaseTime");
        final Data keyData = toData(key);
        ClientMessage request = MultiMapLockWithLeaseTimeParameters.encode(name, keyData, ThreadUtil.getThreadId(), getTimeInMillis(leaseTime, timeUnit));
        invoke(request, keyData);
    }

    public boolean isLocked(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        ClientMessage request = MultiMapIsLockedParameters.encode(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public boolean tryLock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        try {
            return tryLock(key, 0, null);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public boolean tryLock(K key, long time, TimeUnit timeunit) throws InterruptedException {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        ClientMessage request = MultiMapTryLockParameters.encode(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        BooleanResultParameters resultParameters = BooleanResultParameters.decode(response);
        return resultParameters.result;
    }

    public void unlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        ClientMessage request = MultiMapUnlockParameters.encode(name, keyData, ThreadUtil.getThreadId());
        invoke(request, keyData);
    }

    public void forceUnlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        ClientMessage request = MultiMapForceUnlockParameters.encode(name, keyData, ThreadUtil.getThreadId());
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

    private Collection toObjectCollection(PortableCollection result) {
        final Collection<Data> resultCollection = result.getCollection();
        // create a fresh instance of same collection type.
        final Collection newCollection = createCollection(resultCollection);
        for (Data data : resultCollection) {
            newCollection.add(toObject(data));
        }
        return newCollection;
    }

    protected long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    private EventHandler<ClientMessage> createHandler(final Object listener, final boolean includeValue) {
        final ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        return new ClientMultiMapEventHandler(listenerAdaptor, includeValue);
    }

    @Override
    public String toString() {
        return "MultiMap{" + "name='" + getName() + '\'' + '}';
    }

    private class ClientMultiMapEventHandler implements EventHandler<ClientMessage> {

        private final ListenerAdapter listenerAdapter;
        private final boolean includeValue;

        public ClientMultiMapEventHandler(ListenerAdapter listenerAdapter, boolean includeValue) {
            this.listenerAdapter = listenerAdapter;
            this.includeValue = includeValue;
        }

        public void handle(ClientMessage clientMessage) {
            AddEntryListenerEventParameters event = AddEntryListenerEventParameters.decode(clientMessage);

            Member member = getContext().getClusterService().getMember(event.uuid);
            final IMapEvent iMapEvent = createIMapEvent(event, member);
            listenerAdapter.onEvent(iMapEvent);
        }

        private IMapEvent createIMapEvent(AddEntryListenerEventParameters event, Member member) {
            IMapEvent iMapEvent;
            EntryEventType entryEventType = EntryEventType.getByType(event.eventType);
            switch (entryEventType) {
                case ADDED:
                case REMOVED:
                    iMapEvent = createEntryEvent(event, member);
                    break;
                case CLEAR_ALL:
                    iMapEvent = createMapEvent(event, member);
                    break;
                default:
                    throw new IllegalArgumentException("Not a known event type " + entryEventType);
            }

            return iMapEvent;
        }

        private MapEvent createMapEvent(AddEntryListenerEventParameters event, Member member) {
            return new MapEvent(name, member, event.eventType, event.numberOfAffectedEntries);
        }

        private EntryEvent<K, V> createEntryEvent(AddEntryListenerEventParameters event, Member member) {
            V value = null;
            V oldValue = null;
            if (includeValue) {
                value = toObject(event.value);
                oldValue = toObject(event.oldValue);
            }
            K key = toObject(event.key);
            return new EntryEvent<K, V>(name, member, event.eventType, key, oldValue, value);
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }
    }
}
