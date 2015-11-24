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
import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.map.impl.nearcache.ClientHeapNearCache;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
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
import com.hazelcast.replicatedmap.impl.client.ClientReplicatedMapAddEntryListenerRequest;
import com.hazelcast.replicatedmap.impl.client.ClientReplicatedMapAddNearCacheListenerRequest;
import com.hazelcast.replicatedmap.impl.client.ClientReplicatedMapClearRequest;
import com.hazelcast.replicatedmap.impl.client.ClientReplicatedMapContainsKeyRequest;
import com.hazelcast.replicatedmap.impl.client.ClientReplicatedMapContainsValueRequest;
import com.hazelcast.replicatedmap.impl.client.ClientReplicatedMapEntrySetRequest;
import com.hazelcast.replicatedmap.impl.client.ClientReplicatedMapGetRequest;
import com.hazelcast.replicatedmap.impl.client.ClientReplicatedMapIsEmptyRequest;
import com.hazelcast.replicatedmap.impl.client.ClientReplicatedMapKeySetRequest;
import com.hazelcast.replicatedmap.impl.client.ClientReplicatedMapPutAllRequest;
import com.hazelcast.replicatedmap.impl.client.ClientReplicatedMapPutTtlRequest;
import com.hazelcast.replicatedmap.impl.client.ClientReplicatedMapRemoveEntryListenerRequest;
import com.hazelcast.replicatedmap.impl.client.ClientReplicatedMapRemoveRequest;
import com.hazelcast.replicatedmap.impl.client.ClientReplicatedMapSizeRequest;
import com.hazelcast.replicatedmap.impl.client.ClientReplicatedMapValuesRequest;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapEntries;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapKeys;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapPortableEntryEvent;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapValueCollection;
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

/**
 * The replicated map client side proxy implementation proxying all requests to a member node
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ClientReplicatedMapProxy<K, V> extends ClientProxy implements ReplicatedMap<K, V> {

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
        Data dataKey = toData(key);
        Data dataValue = toData(value);
        ClientReplicatedMapPutTtlRequest request = new ClientReplicatedMapPutTtlRequest(getName(), dataKey, dataValue,
                timeUnit.toMillis(ttl));
        return (V) invoke(request, dataKey);
    }

    @Override
    public int size() {
        ClientReplicatedMapSizeRequest request = new ClientReplicatedMapSizeRequest(getName(),
                getOrInitTargetPartitionId());
        return (Integer) invokeOnPartition(request, getOrInitTargetPartitionId());
    }

    @Override
    public boolean isEmpty() {
        ClientReplicatedMapIsEmptyRequest request = new ClientReplicatedMapIsEmptyRequest(getName(),
                getOrInitTargetPartitionId());
        return (Boolean) invokeOnPartition(request, getOrInitTargetPartitionId());
    }

    @Override
    public boolean containsKey(Object key) {
        Data dataKey = toData(key);
        return (Boolean) invoke(new ClientReplicatedMapContainsKeyRequest(getName(), dataKey), dataKey);
    }

    @Override
    public boolean containsValue(Object value) {
        Data dataValue = toData(value);
        ClientReplicatedMapContainsValueRequest request = new ClientReplicatedMapContainsValueRequest(getName(),
                dataValue, getOrInitTargetPartitionId());
        return (Boolean) invokeOnPartition(request, getOrInitTargetPartitionId());
    }

    @Override
    public V get(Object key) {
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
        Data dataKey = toData(key);
        ClientReplicatedMapGetRequest request = new ClientReplicatedMapGetRequest(getName(), dataKey);
        Object response = invoke(request, dataKey);
        V value = response != null ? (V) response : null;
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
        Data dataKey = toData(key);
        ClientReplicatedMapRemoveRequest request = new ClientReplicatedMapRemoveRequest(getName(), dataKey);
        return (V) invoke(request, dataKey);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        Set<? extends Entry<? extends K, ? extends V>> entries = m.entrySet();
        List<Entry<Data, Data>> dataEntries = new ArrayList<Entry<Data, Data>>(m.size());
        for (Entry<? extends K, ? extends V> entry : entries) {
            dataEntries.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(toData(entry.getKey()),
                    toData(entry.getValue())));
        }
        ReplicatedMapEntries entrySet = new ReplicatedMapEntries(dataEntries);
        ClientReplicatedMapPutAllRequest req = new ClientReplicatedMapPutAllRequest(getName(), entrySet);
        invoke(req);
    }

    @Override
    public void clear() {
        ClientReplicatedMapClearRequest request = new ClientReplicatedMapClearRequest(getName(),
                getOrInitTargetPartitionId());
        invokeOnPartition(request, getOrInitTargetPartitionId());
    }

    @Override
    public boolean removeEntryListener(String id) {
        return deregisterListener(id);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener) {
        ClientReplicatedMapAddEntryListenerRequest addRequest =
                new ClientReplicatedMapAddEntryListenerRequest(getName(), null, null);
        EventHandler<ReplicatedMapPortableEntryEvent> handler = createHandler(listener);
        ClientReplicatedMapRemoveEntryListenerRequest removeRequest =
                new ClientReplicatedMapRemoveEntryListenerRequest(getName());
        return registerListener(addRequest, removeRequest, handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, K key) {
        Data dataKey = toData(key);
        ClientReplicatedMapAddEntryListenerRequest addRequest =
                new ClientReplicatedMapAddEntryListenerRequest(getName(), null, dataKey);
        ClientReplicatedMapRemoveEntryListenerRequest removeRequest =
                new ClientReplicatedMapRemoveEntryListenerRequest(getName());
        EventHandler<ReplicatedMapPortableEntryEvent> handler = createHandler(listener);
        return registerListener(addRequest, removeRequest, handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate) {
        ClientReplicatedMapAddEntryListenerRequest addRequest = new ClientReplicatedMapAddEntryListenerRequest(getName()
                , predicate, null);
        EventHandler<ReplicatedMapPortableEntryEvent> handler = createHandler(listener);
        ClientReplicatedMapRemoveEntryListenerRequest removeRequest =
                new ClientReplicatedMapRemoveEntryListenerRequest(getName());
        return registerListener(addRequest, removeRequest, handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key) {
        Data dataKey = toData(key);
        ClientReplicatedMapAddEntryListenerRequest addRequest =
                new ClientReplicatedMapAddEntryListenerRequest(getName(), predicate, dataKey);
        EventHandler<ReplicatedMapPortableEntryEvent> handler = createHandler(listener);
        ClientReplicatedMapRemoveEntryListenerRequest removeRequest =
                new ClientReplicatedMapRemoveEntryListenerRequest(getName());
        return registerListener(addRequest, removeRequest, handler);
    }

    @Override
    public Set<K> keySet() {
        ClientReplicatedMapKeySetRequest request = new ClientReplicatedMapKeySetRequest(getName(),
                getOrInitTargetPartitionId());
        ReplicatedMapKeys response = invokeOnPartition(request, getOrInitTargetPartitionId());
        List<Data> dataKeys = response.getKeys();
        List<Entry<K, V>> keys = new ArrayList<Entry<K, V>>(dataKeys.size());
        for (Data dataKey : dataKeys) {
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
        ClientReplicatedMapValuesRequest request = new ClientReplicatedMapValuesRequest(getName(),
                getOrInitTargetPartitionId());
        ReplicatedMapValueCollection result = invokeOnPartition(request, getOrInitTargetPartitionId());
        Collection<Data> dataValues = result.getValues();
        ArrayList<V> values = new ArrayList<V>(dataValues.size());
        for (Data dataValue : dataValues) {
            values.add((V) toObject(dataValue));
        }
        return values;
    }

    @Override
    public Collection<V> values(Comparator<V> comparator) {
        List values = (List) values();
        Collections.sort(values, comparator);
        return values;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        ClientReplicatedMapEntrySetRequest request = new ClientReplicatedMapEntrySetRequest(getName(),
                getOrInitTargetPartitionId());
        ReplicatedMapEntries response = invokeOnPartition(request, getOrInitTargetPartitionId());
        List<Entry<Data, Data>> dataEntries = response.getEntries();
        List<Entry<K, V>> entries = new ArrayList<Entry<K, V>>(dataEntries.size());
        for (Entry<Data, Data> dataEntry : dataEntries) {
            K key = toObject(dataEntry.getKey());
            V value = toObject(dataEntry.getValue());
            entries.add(new AbstractMap.SimpleImmutableEntry<K, V>(key, value));
        }
        return new ResultSet(entries, IterationType.ENTRY);
    }

    private EventHandler<ReplicatedMapPortableEntryEvent> createHandler(final EntryListener<K, V> listener) {
        return new ReplicatedMapEventHandler(listener);
    }

    private void initNearCache() {
        if (nearCacheInitialized.compareAndSet(false, true)) {
            final NearCacheConfig nearCacheConfig = getContext().getClientConfig().getNearCacheConfig(getName());
            if (nearCacheConfig == null) {
                return;
            }
            ClientHeapNearCache<Object> nearCache =
                    new ClientHeapNearCache<Object>(getName(), getContext(), nearCacheConfig);
            this.nearCache = nearCache;
            if (nearCache.isInvalidateOnChange()) {
                addNearCacheInvalidateListener();
            }
        }
    }

    private void addNearCacheInvalidateListener() {
        try {
            ClientReplicatedMapAddNearCacheListenerRequest addRequest =
                    new ClientReplicatedMapAddNearCacheListenerRequest(getName());
            BaseClientRemoveListenerRequest removeRequest =
                    new ClientReplicatedMapRemoveEntryListenerRequest(getName());
            EventHandler handler = new ReplicatedMapNearCacheEventHandler();
            invalidationListenerId = registerListener(addRequest, removeRequest, handler);
        } catch (Exception e) {
            Logger.getLogger(ClientHeapNearCache.class).severe(
                    "-----------------\n Near Cache is not initialized!!! \n-----------------", e);
        }
    }

    private void removeNearCacheInvalidationListener() {
        if (nearCache != null && invalidationListenerId != null) {
            deregisterListener(invalidationListenerId);
        }
    }

    @Override
    public String toString() {
        return "ReplicatedMap{" + "name='" + getName() + '\'' + '}';
    }

    private class ReplicatedMapEventHandler implements EventHandler<ReplicatedMapPortableEntryEvent> {
        private final EntryListener<K, V> listener;

        ReplicatedMapEventHandler(EntryListener<K, V> listener) {
            this.listener = listener;
        }

        public void handle(ReplicatedMapPortableEntryEvent event) {
            EntryEventType eventType = event.getEventType();
            Data keyData = event.getKey();
            Data valueData = event.getValue();
            Data oldValueData = event.getOldValue();
            Member member = getContext().getClusterService().getMember(event.getUuid());
            int type = eventType.getType();
            EntryEvent<K, V> entryEvent = new DataAwareEntryEvent(member, type, getName(), keyData, valueData,
                    oldValueData, null, getContext().getSerializationService());

            switch (event.getEventType()) {
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
                    MapEvent mapEvent = new MapEvent(getName(), member, EntryEventType.CLEAR_ALL.getType(),
                            event.getNumberOfAffectedEntries());
                    listener.mapCleared(mapEvent);
                    break;
                default:
                    throw new IllegalArgumentException("Not a known event type " + event.getEventType());
            }
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }
    }

    private void invalidateNearCache() {
        if (nearCache != null) {
            nearCache.clear();
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


    private class ReplicatedMapNearCacheEventHandler implements EventHandler<ReplicatedMapPortableEntryEvent> {
        @Override
        public void handle(ReplicatedMapPortableEntryEvent event) {
            switch (event.getEventType()) {
                case ADDED:
                case REMOVED:
                case UPDATED:
                case EVICTED:
                    nearCache.remove(toObject(event.getKey()));
                    break;
                case CLEAR_ALL:
                    nearCache.clear();
                    break;
                default:
                    throw new IllegalArgumentException("Not a known event type " + event.getEventType());
            }
        }

        @Override
        public void beforeListenerRegister() {
            invalidateNearCache();
        }

        @Override
        public void onListenerRegister() {
            invalidateNearCache();
        }
    }

}
