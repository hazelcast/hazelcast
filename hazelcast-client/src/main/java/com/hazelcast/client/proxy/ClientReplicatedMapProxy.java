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

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerToKeyCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerToKeyWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddEntryListenerWithPredicateCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapAddNearCacheEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapClearCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapContainsKeyCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapContainsValueCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapEntrySetCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapGetCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapIsEmptyCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapKeySetCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapPutAllCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapPutCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapSizeCodec;
import com.hazelcast.client.impl.protocol.codec.ReplicatedMapValuesCodec;
import com.hazelcast.client.map.impl.nearcache.ClientHeapNearCache;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ListenerMessageCodec;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.monitor.LocalReplicatedMapStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.impl.record.ResultSet;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.IterationType;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.hazelcast.cache.impl.nearcache.NearCache.NULL_OBJECT;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * The replicated map client side proxy implementation proxying all requests to a member node
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ClientReplicatedMapProxy<K, V> extends ClientProxy implements ReplicatedMap<K, V> {

    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";

    private static final Random RANDOM_PARTITION_ID_GENERATOR = new Random();

    private static final AtomicIntegerFieldUpdater<ClientReplicatedMapProxy> TARGET_PARTITION_ID_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ClientReplicatedMapProxy.class, "targetPartitionId");

    private volatile NearCache nearCache;
    private volatile String invalidationListenerId;
    private final AtomicBoolean nearCacheInitialized = new AtomicBoolean();

    // all requests are forwarded to the same node via a random partition id
    // do not use this field directly. use getOrInitTargetPartitionId() instead.
    private volatile int targetPartitionId = Operation.GENERIC_PARTITION_ID;


    public ClientReplicatedMapProxy(String serviceName, String objectName) {
        super(serviceName, objectName);
    }

    @Override
    protected void onDestroy() {
        if (nearCache != null) {
            removeNearCacheInvalidationListener();
            nearCache.destroy();
        }
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        Data keyData = toData(key);
        ClientMessage request = ReplicatedMapPutCodec.encodeRequest(name, keyData, valueData, timeUnit.toMillis(ttl));
        ClientMessage response = invoke(request, keyData);
        ReplicatedMapPutCodec.ResponseParameters result = ReplicatedMapPutCodec.decodeResponse(response);
        return toObject(result.response);
    }

    @Override
    public int size() {
        ClientMessage request = ReplicatedMapSizeCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request, getOrInitTargetPartitionId());
        ReplicatedMapSizeCodec.ResponseParameters result = ReplicatedMapSizeCodec.decodeResponse(response);
        return result.response;
    }

    @Override
    public boolean isEmpty() {
        ClientMessage request = ReplicatedMapIsEmptyCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request, getOrInitTargetPartitionId());
        ReplicatedMapIsEmptyCodec.ResponseParameters result = ReplicatedMapIsEmptyCodec.decodeResponse(response);
        return result.response;
    }

    @Override
    public boolean containsKey(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        ClientMessage request = ReplicatedMapContainsKeyCodec.encodeRequest(name, keyData);
        ClientMessage response = invoke(request, keyData);
        ReplicatedMapContainsKeyCodec.ResponseParameters result = ReplicatedMapContainsKeyCodec.decodeResponse(response);
        return result.response;
    }

    @Override
    public boolean containsValue(Object value) {
        checkNotNull(value, NULL_KEY_IS_NOT_ALLOWED);
        Data valueData = toData(value);
        ClientMessage request = ReplicatedMapContainsValueCodec.encodeRequest(name, valueData);
        ClientMessage response = invokeOnPartition(request, getOrInitTargetPartitionId());
        ReplicatedMapContainsValueCodec.ResponseParameters result = ReplicatedMapContainsValueCodec.decodeResponse(response);
        return result.response;
    }

    @Override
    public V get(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        initNearCache();
        if (nearCache != null) {
            Object cached = nearCache.get(key);
            if (cached != null) {
                if (cached.equals(NULL_OBJECT)) {
                    return null;
                }
                return (V) cached;
            }
        }

        Data keyData = toData(key);
        ClientMessage request = ReplicatedMapGetCodec.encodeRequest(name, keyData);
        ClientMessage response = invoke(request, keyData);

        ReplicatedMapGetCodec.ResponseParameters result = ReplicatedMapGetCodec.decodeResponse(response);

        V value = (V) toObject(result.response);
        if (nearCache != null) {
            nearCache.put(key, value);
        }
        return value;
    }

    @Override
    public V put(K key, V value) {
        return put(key, value, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public V remove(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        ClientMessage request = ReplicatedMapRemoveCodec.encodeRequest(name, keyData);
        ClientMessage response = invoke(request, keyData);
        ReplicatedMapRemoveCodec.ResponseParameters result = ReplicatedMapRemoveCodec.decodeResponse(response);
        return toObject(result.response);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        List<Entry<Data, Data>> dataEntries = new ArrayList<Entry<Data, Data>>(m.size());
        for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
            dataEntries.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(toData(entry.getKey()),
                    toData(entry.getValue())));
        }

        ClientMessage request = ReplicatedMapPutAllCodec.encodeRequest(name, dataEntries);
        invoke(request);
    }

    @Override
    public void clear() {
        ClientMessage request = ReplicatedMapClearCodec.encodeRequest(name);
        invoke(request);
    }

    @Override
    public boolean removeEntryListener(String registrationId) {
        return deregisterListener(registrationId);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener) {
        EventHandler<ClientMessage> handler = createHandler(listener);
        return registerListener(createEntryListenerCodec(), handler);
    }

    private ListenerMessageCodec createEntryListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return ReplicatedMapAddEntryListenerCodec.encodeRequest(name, localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return ReplicatedMapAddEntryListenerCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return ReplicatedMapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return ReplicatedMapRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        EventHandler<ClientMessage> handler = createHandler(listener);
        return registerListener(createEntryListenerToKeyCodec(keyData), handler);
    }

    private ListenerMessageCodec createEntryListenerToKeyCodec(final Data keyData) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return ReplicatedMapAddEntryListenerToKeyCodec.encodeRequest(name, keyData, localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return ReplicatedMapAddEntryListenerToKeyCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return ReplicatedMapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return ReplicatedMapRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate) {
        final Data predicateData = toData(predicate);
        EventHandler<ClientMessage> handler = createHandler(listener);
        return registerListener(createEntryListenerWithPredicateCodec(predicateData), handler);
    }

    private ListenerMessageCodec createEntryListenerWithPredicateCodec(final Data predicateData) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return ReplicatedMapAddEntryListenerWithPredicateCodec.encodeRequest(name, predicateData, localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return ReplicatedMapAddEntryListenerWithPredicateCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return ReplicatedMapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return ReplicatedMapRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        final Data predicateData = toData(predicate);
        EventHandler<ClientMessage> handler = createHandler(listener);
        return registerListener(createEntryListenerToKeyWithPredicateCodec(keyData, predicateData), handler);
    }

    private ListenerMessageCodec createEntryListenerToKeyWithPredicateCodec(final Data keyData,
                                                                            final Data predicateData) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return ReplicatedMapAddEntryListenerToKeyWithPredicateCodec
                        .encodeRequest(name, keyData, predicateData, localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return ReplicatedMapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return ReplicatedMapRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    @Override
    public Set<K> keySet() {
        ClientMessage request = ReplicatedMapKeySetCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request, getOrInitTargetPartitionId());
        ReplicatedMapKeySetCodec.ResponseParameters result = ReplicatedMapKeySetCodec.decodeResponse(response);
        List<Entry<K, V>> keys = new ArrayList<Entry<K, V>>(result.response.size());
        for (Data dataKey : result.response) {
            keys.add(new AbstractMap.SimpleImmutableEntry<K, V>((K) toObject(dataKey), null));
        }
        return new ResultSet(keys, IterationType.KEY);
    }

    @Override
    public LocalReplicatedMapStats getReplicatedMapStats() {
        throw new UnsupportedOperationException("Replicated Map statistics are not available for client !");
    }

    @Override
    public Collection<V> values() {
        ClientMessage request = ReplicatedMapValuesCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request, getOrInitTargetPartitionId());
        ReplicatedMapValuesCodec.ResponseParameters result = ReplicatedMapValuesCodec.decodeResponse(response);
        Collection<V> resultCollection = new ArrayList<V>(result.response.size());
        for (Data data : result.response) {
            resultCollection.add((V) toObject(data));
        }
        return resultCollection;
    }

    @Override
    public Collection<V> values(Comparator<V> comparator) {
        List values = (List) values();
        Collections.sort(values, comparator);
        return values;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        ClientMessage request = ReplicatedMapEntrySetCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request, getOrInitTargetPartitionId());
        ReplicatedMapEntrySetCodec.ResponseParameters result = ReplicatedMapEntrySetCodec.decodeResponse(response);
        List<Entry<K, V>> entries = new ArrayList<Entry<K, V>>(result.response.size());
        for (Entry<Data, Data> dataEntry : result.response) {
            K key = toObject(dataEntry.getKey());
            V value = toObject(dataEntry.getValue());
            entries.add(new AbstractMap.SimpleImmutableEntry<K, V>(key, value));
        }
        return new ResultSet<K, V>(entries, IterationType.ENTRY);
    }

    private EventHandler<ClientMessage> createHandler(final EntryListener<K, V> listener) {
        return new ReplicatedMapEventHandler(listener);
    }

    private void initNearCache() {
        if (nearCacheInitialized.compareAndSet(false, true)) {
            final NearCacheConfig nearCacheConfig = getContext().getClientConfig().getNearCacheConfig(name);
            if (nearCacheConfig == null) {
                return;
            }
            ClientHeapNearCache<Object> nearCache = new ClientHeapNearCache<Object>(name,
                    getContext(), nearCacheConfig);
            this.nearCache = nearCache;
            if (nearCache.isInvalidateOnChange()) {
                addNearCacheInvalidateListener();
            }
        }
    }

    private void addNearCacheInvalidateListener() {
        try {
            EventHandler handler = new ReplicatedMapAddNearCacheEventHandler();
            invalidationListenerId = registerListener(createNearCacheInvalidationListenerCodec(), handler);
        } catch (Exception e) {
            Logger.getLogger(ClientHeapNearCache.class).severe(
                    "-----------------\n Near Cache is not initialized!!! \n-----------------", e);
        }
    }

    private ListenerMessageCodec createNearCacheInvalidationListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return ReplicatedMapAddNearCacheEntryListenerCodec.encodeRequest(name, false, localOnly);
            }

            @Override
            public String decodeAddResponse(ClientMessage clientMessage) {
                return ReplicatedMapAddNearCacheEntryListenerCodec.decodeResponse(clientMessage).response;
            }

            @Override
            public ClientMessage encodeRemoveRequest(String realRegistrationId) {
                return ReplicatedMapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return ReplicatedMapRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
    }

    private void removeNearCacheInvalidationListener() {
        if (nearCache != null && invalidationListenerId != null) {
            getContext().getListenerService().deregisterListener(invalidationListenerId);
        }
    }

    @Override
    public String toString() {
        return "ReplicatedMap{" + "name='" + name + '\'' + '}';
    }

    private class ReplicatedMapEventHandler extends ReplicatedMapAddEntryListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {
        private final EntryListener<K, V> listener;

        ReplicatedMapEventHandler(EntryListener<K, V> listener) {
            this.listener = listener;
        }

        @Override
        public void handle(Data keyData, Data valueData, Data oldValueData, Data mergingValue,
                           int eventTypeId, String uuid, int numberOfAffectedEntries) {
            Member member = getContext().getClusterService().getMember(uuid);
            EntryEventType eventType = EntryEventType.getByType(eventTypeId);
            EntryEvent<K, V> entryEvent = new DataAwareEntryEvent(member, eventTypeId, name, keyData,
                    valueData, oldValueData, null, getContext().getSerializationService());
            switch (eventType) {
                case ADDED:
                    listener.entryAdded(entryEvent);
                    break;
                case REMOVED:
                    listener.entryRemoved(entryEvent);
                    break;
                case UPDATED:
                    listener.entryUpdated(entryEvent);
                    break;
                case EVICTED:
                    listener.entryEvicted(entryEvent);
                    break;
                case CLEAR_ALL:
                    MapEvent mapEvent = new MapEvent(getName(), member, eventTypeId, numberOfAffectedEntries);
                    listener.mapCleared(mapEvent);
                    break;
                default:
                    throw new IllegalArgumentException("Not a known event type " + eventType);
            }
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }
    }

    private int getOrInitTargetPartitionId() {
        int targetPartitionId = this.targetPartitionId;
        while (targetPartitionId == Operation.GENERIC_PARTITION_ID) {
            final int partitionCount = getContext().getPartitionService().getPartitionCount();
            targetPartitionId = RANDOM_PARTITION_ID_GENERATOR.nextInt(partitionCount);
            if (!TARGET_PARTITION_ID_UPDATER.compareAndSet(this, -1, targetPartitionId)) {
                targetPartitionId = this.targetPartitionId;
            }
        }

        return targetPartitionId;
    }

    private class ReplicatedMapAddNearCacheEventHandler
            extends ReplicatedMapAddNearCacheEntryListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        @Override
        public void beforeListenerRegister() {
            if (nearCache != null) {
                nearCache.clear();
            }
        }

        @Override
        public void onListenerRegister() {
            if (nearCache != null) {
                nearCache.clear();
            }
        }

        @Override
        public void handle(Data key, Data value, Data oldValue, Data mergingValue,
                           int eventType, String uuid, int numberOfAffectedEntries) {
            EntryEventType entryEventType = EntryEventType.getByType(eventType);
            switch (entryEventType) {
                case ADDED:
                case REMOVED:
                case UPDATED:
                case EVICTED:
                    nearCache.remove(toObject(key));
                    break;
                case CLEAR_ALL:
                    nearCache.clear();
                    break;
                default:
                    throw new IllegalArgumentException("Not a known event type " + entryEventType);
            }
        }
    }
}
