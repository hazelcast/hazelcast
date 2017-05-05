/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.ClientLockReferenceIdGenerator;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MultiMapAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapAddEntryListenerToKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapClearCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapContainsEntryCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapContainsKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapContainsValueCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapEntrySetCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapForceUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapGetCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapIsLockedCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapKeySetCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapLockCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapPutCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapRemoveEntryCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapSizeCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapTryLockCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapValueCountCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapValuesCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.map.impl.ListenerAdapter;
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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.UnmodifiableLazyList;
import com.hazelcast.util.Preconditions;
import com.hazelcast.util.ThreadUtil;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.ListenerAdapters.createListenerAdapter;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Proxy implementation of {@link MultiMap}.
 *
 * @param <K> key
 * @param <V> value
 * @author ali 5/19/13
 */
public class ClientMultiMapProxy<K, V> extends ClientProxy implements MultiMap<K, V> {

    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";

    private ClientLockReferenceIdGenerator lockReferenceIdGenerator;

    public ClientMultiMapProxy(String serviceName, String name, ClientContext context) {
        super(serviceName, name, context);
    }

    @Override
    public boolean put(K key, V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        Data valueData = toData(value);
        ClientMessage request = MultiMapPutCodec.encodeRequest(name, keyData, valueData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        MultiMapPutCodec.ResponseParameters resultParameters = MultiMapPutCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public Collection<V> get(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        ClientMessage request = MultiMapGetCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        MultiMapGetCodec.ResponseParameters resultParameters = MultiMapGetCodec.decodeResponse(response);
        return new UnmodifiableLazyList<V>(resultParameters.response, getSerializationService());
    }

    @Override
    public boolean remove(Object key, Object value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        Data valueData = toData(value);
        ClientMessage request = MultiMapRemoveEntryCodec.encodeRequest(name, keyData, valueData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        MultiMapRemoveEntryCodec.ResponseParameters resultParameters = MultiMapRemoveEntryCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public Collection<V> remove(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        ClientMessage request = MultiMapRemoveCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        MultiMapRemoveCodec.ResponseParameters resultParameters = MultiMapRemoveCodec.decodeResponse(response);
        return new UnmodifiableLazyList<V>(resultParameters.response, getSerializationService());
    }

    @Override
    public Set<K> localKeySet() {
        throw new UnsupportedOperationException("Locality for client is ambiguous");
    }

    @Override
    public Set<K> keySet() {
        ClientMessage request = MultiMapKeySetCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        MultiMapKeySetCodec.ResponseParameters resultParameters = MultiMapKeySetCodec.decodeResponse(response);
        Collection<Data> result = resultParameters.response;
        Set<K> keySet = new HashSet<K>(result.size());
        for (Data data : result) {
            final K key = toObject(data);
            keySet.add(key);
        }
        return keySet;
    }

    @Override
    public Collection<V> values() {
        ClientMessage request = MultiMapValuesCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        MultiMapValuesCodec.ResponseParameters resultParameters = MultiMapValuesCodec.decodeResponse(response);
        return new UnmodifiableLazyList<V>(resultParameters.response, getSerializationService());
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        ClientMessage request = MultiMapEntrySetCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        MultiMapEntrySetCodec.ResponseParameters resultParameters = MultiMapEntrySetCodec.decodeResponse(response);

        Set<Map.Entry<K, V>> entrySet = new HashSet<Map.Entry<K, V>>(resultParameters.response.size());
        for (Map.Entry<Data, Data> entry : resultParameters.response) {
            K key = toObject(entry.getKey());
            V value = toObject(entry.getValue());
            entrySet.add(new AbstractMap.SimpleEntry<K, V>(key, value));
        }
        return entrySet;
    }

    @Override
    public boolean containsKey(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        ClientMessage request = MultiMapContainsKeyCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        MultiMapContainsKeyCodec.ResponseParameters resultParameters = MultiMapContainsKeyCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean containsValue(Object value) {
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyValue = toData(value);
        ClientMessage request = MultiMapContainsValueCodec.encodeRequest(name, keyValue);
        ClientMessage response = invoke(request);
        MultiMapContainsValueCodec.ResponseParameters resultParameters = MultiMapContainsValueCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean containsEntry(K key, V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        Data valueData = toData(value);
        ClientMessage request = MultiMapContainsEntryCodec.encodeRequest(name, keyData, valueData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        MultiMapContainsEntryCodec.ResponseParameters resultParameters = MultiMapContainsEntryCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public int size() {
        ClientMessage request = MultiMapSizeCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        MultiMapSizeCodec.ResponseParameters resultParameters = MultiMapSizeCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public void clear() {
        ClientMessage request = MultiMapClearCodec.encodeRequest(name);
        invoke(request);
    }

    @Override
    public int valueCount(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        ClientMessage request = MultiMapValueCountCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        MultiMapValueCountCodec.ResponseParameters resultParameters = MultiMapValueCountCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public String addLocalEntryListener(EntryListener<K, V> listener) {
        throw new UnsupportedOperationException("Locality for client is ambiguous");
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, final boolean includeValue) {
        isNotNull(listener, "listener");
        EventHandler<ClientMessage> handler = createHandler(listener);
        return registerListener(createEntryListenerCodec(includeValue), handler);
    }

    private ListenerMessageCodec createEntryListenerCodec(final boolean includeValue) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return MultiMapAddEntryListenerCodec.encodeRequest(name, includeValue, localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return MultiMapAddEntryListenerCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return MultiMapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MultiMapRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    @Override
    public boolean removeEntryListener(String registrationId) {
        return deregisterListener(registrationId);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, K key, final boolean includeValue) {
        final Data keyData = toData(key);
        EventHandler<ClientMessage> handler = createHandler(listener);
        return registerListener(createEntryListenerToKeyCodec(includeValue, keyData), handler);
    }

    private ListenerMessageCodec createEntryListenerToKeyCodec(final boolean includeValue, final Data keyData) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return MultiMapAddEntryListenerToKeyCodec.encodeRequest(name, keyData, includeValue, localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return MultiMapAddEntryListenerToKeyCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return MultiMapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MultiMapRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    @Override
    public void lock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        ClientMessage request = MultiMapLockCodec
                .encodeRequest(name, keyData, ThreadUtil.getThreadId(), -1, lockReferenceIdGenerator.getNextReferenceId());
        invoke(request, keyData);
    }

    @Override
    public void lock(K key, long leaseTime, TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkPositive(leaseTime, "leaseTime should be positive");

        final Data keyData = toData(key);
        ClientMessage request = MultiMapLockCodec
                .encodeRequest(name, keyData, ThreadUtil.getThreadId(), getTimeInMillis(leaseTime, timeUnit),
                        lockReferenceIdGenerator.getNextReferenceId());
        invoke(request, keyData);
    }

    @Override
    public boolean isLocked(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        ClientMessage request = MultiMapIsLockedCodec.encodeRequest(name, keyData);
        ClientMessage response = invoke(request, keyData);
        MultiMapIsLockedCodec.ResponseParameters resultParameters = MultiMapIsLockedCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public boolean tryLock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        try {
            return tryLock(key, 0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    @Override
    public boolean tryLock(K key, long time, TimeUnit timeunit) throws InterruptedException {
        return tryLock(key, time, timeunit, Long.MAX_VALUE, null);
    }

    @Override
    public boolean tryLock(K key, long time, TimeUnit timeunit, long leaseTime, TimeUnit leaseUnit) throws InterruptedException {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        long timeoutInMillis = getTimeInMillis(time, timeunit);
        long leaseTimeInMillis = getTimeInMillis(leaseTime, leaseUnit);

        long threadId = ThreadUtil.getThreadId();
        ClientMessage request = MultiMapTryLockCodec.encodeRequest(name, keyData, threadId, leaseTimeInMillis, timeoutInMillis,
                lockReferenceIdGenerator.getNextReferenceId());
        ClientMessage response = invoke(request, keyData);
        MultiMapTryLockCodec.ResponseParameters resultParameters = MultiMapTryLockCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public void unlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        ClientMessage request = MultiMapUnlockCodec
                .encodeRequest(name, keyData, ThreadUtil.getThreadId(), lockReferenceIdGenerator.getNextReferenceId());
        invoke(request, keyData);
    }

    @Override
    public void forceUnlock(K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        ClientMessage request = MultiMapForceUnlockCodec
                .encodeRequest(name, keyData, lockReferenceIdGenerator.getNextReferenceId());
        invoke(request, keyData);
    }

    @Override
    public LocalMultiMapStats getLocalMultiMapStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
    }

    @Override
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation) {

        JobTracker jobTracker = getClient().getJobTracker("hz::aggregation-multimap-" + name);
        return aggregate(supplier, aggregation, jobTracker);
    }

    @Override
    public <SuppliedValue, Result> Result aggregate(Supplier<K, V, SuppliedValue> supplier,
                                                    Aggregation<K, SuppliedValue, Result> aggregation,
                                                    JobTracker jobTracker) {

        try {
            Preconditions.isNotNull(jobTracker, "jobTracker");
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

    @Override
    public String toString() {
        return "MultiMap{" + "name='" + name + '\'' + '}';
    }

    protected void onDestroy() {
    }

    protected long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    @Override
    protected void onInitialize() {
        super.onInitialize();

        lockReferenceIdGenerator = getClient().getLockReferenceIdGenerator();
    }

    private EventHandler<ClientMessage> createHandler(final Object listener) {
        final ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        return new ClientMultiMapEventHandler(listenerAdaptor);
    }

    private class ClientMultiMapEventHandler extends MultiMapAddEntryListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final ListenerAdapter listenerAdapter;

        public ClientMultiMapEventHandler(ListenerAdapter listenerAdapter) {
            this.listenerAdapter = listenerAdapter;
        }

        @Override
        public void handle(Data key, Data value, Data oldValue, Data mergingValue,
                           int eventType, String uuid, int numberOfAffectedEntries) {
            Member member = getContext().getClusterService().getMember(uuid);
            final IMapEvent iMapEvent = createIMapEvent(key, value, oldValue,
                    mergingValue, eventType, numberOfAffectedEntries, member);
            listenerAdapter.onEvent(iMapEvent);
        }

        private IMapEvent createIMapEvent(Data key, Data value, Data oldValue, Data mergingValue,
                                          int eventType, int numberOfAffectedEntries, Member member) {
            IMapEvent iMapEvent;
            EntryEventType entryEventType = EntryEventType.getByType(eventType);
            switch (entryEventType) {
                case ADDED:
                case REMOVED:
                case UPDATED:
                case EVICTED:
                case MERGED:
                    iMapEvent = createEntryEvent(key, value, oldValue, mergingValue, eventType, member);
                    break;
                case EVICT_ALL:
                case CLEAR_ALL:
                    iMapEvent = createMapEvent(eventType, numberOfAffectedEntries, member);
                    break;
                default:
                    throw new IllegalArgumentException("Not a known event type " + entryEventType);
            }

            return iMapEvent;
        }

        private MapEvent createMapEvent(int eventType, int numberOfAffectedEntries, Member member) {
            return new MapEvent(name, member, eventType, numberOfAffectedEntries);
        }

        private EntryEvent<K, V> createEntryEvent(Data keyData, Data valueData, Data oldValueData,
                                                  Data mergingValueData, int eventType, Member member) {
            return new DataAwareEntryEvent<K, V>(member, eventType, name, keyData, valueData, oldValueData, mergingValueData,
                    getSerializationService());
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }
    }
}
