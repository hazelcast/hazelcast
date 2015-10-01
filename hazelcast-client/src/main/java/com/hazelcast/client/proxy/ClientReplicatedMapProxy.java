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

import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.nearcache.ClientHeapNearCache;
import com.hazelcast.client.nearcache.ClientNearCache;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.logging.Logger;
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
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapEntrySet;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapGetResponse;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapKeySet;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapPortableEntryEvent;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapValueCollection;
import com.hazelcast.spi.Operation;

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

/**
 * The replicated map client side proxy implementation proxying all requests to a member node
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ClientReplicatedMapProxy<K, V>
        extends ClientProxy
        implements ReplicatedMap<K, V> {

    private static final Random RANDOM_PARTITION_ID_GENERATOR = new Random();

    private static final AtomicIntegerFieldUpdater<ClientReplicatedMapProxy> TARGET_PARTITION_ID_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ClientReplicatedMapProxy.class, "targetPartitionId");

    private volatile ClientHeapNearCache<Object> nearCache;
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
        final ClientReplicatedMapPutTtlRequest request = new ClientReplicatedMapPutTtlRequest(getName(), key, value,
                timeUnit.toMillis(ttl));
        return invokeOnPartition(request, getOrInitTargetPartitionId());
    }

    @Override
    public int size() {
        return (Integer) invokeOnPartition(new ClientReplicatedMapSizeRequest(getName()), getOrInitTargetPartitionId());
    }

    @Override
    public boolean isEmpty() {
        return (Boolean) invokeOnPartition(new ClientReplicatedMapIsEmptyRequest(getName()), getOrInitTargetPartitionId());
    }

    @Override
    public boolean containsKey(Object key) {
        final ClientReplicatedMapContainsKeyRequest request = new ClientReplicatedMapContainsKeyRequest(getName(), key);
        return (Boolean) invokeOnPartition(request, getOrInitTargetPartitionId());
    }

    @Override
    public boolean containsValue(Object value) {
        final ClientReplicatedMapContainsValueRequest request = new ClientReplicatedMapContainsValueRequest(getName(), value);
        return (Boolean) invokeOnPartition(request, getOrInitTargetPartitionId());
    }

    @Override
    public V get(Object key) {
        initNearCache();
        if (nearCache != null) {
            Object cached = nearCache.get(key);
            if (cached != null) {
                if (cached.equals(ClientNearCache.NULL_OBJECT)) {
                    return null;
                }
                return (V) cached;
            }
        }

        ClientReplicatedMapGetRequest request = new ClientReplicatedMapGetRequest(getName(), key);
        ReplicatedMapGetResponse response = invokeOnPartition(request, getOrInitTargetPartitionId());

        V value = (V) response.getValue();
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
        return invokeOnPartition(new ClientReplicatedMapRemoveRequest(getName(), key), getOrInitTargetPartitionId());
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        final ClientReplicatedMapPutAllRequest request = new ClientReplicatedMapPutAllRequest(getName(),
                new ReplicatedMapEntrySet(m.entrySet()));
        invokeOnPartition(request, getOrInitTargetPartitionId());
    }

    @Override
    public void clear() {
        invokeOnPartition(new ClientReplicatedMapClearRequest(getName()), getOrInitTargetPartitionId());
    }

    @Override
    public boolean removeEntryListener(String id) {
        final ClientReplicatedMapRemoveEntryListenerRequest request =
                new ClientReplicatedMapRemoveEntryListenerRequest(getName(), id);
        return stopListeningOnPartition(request, id, getOrInitTargetPartitionId());
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener) {
        ClientReplicatedMapAddEntryListenerRequest request =
                new ClientReplicatedMapAddEntryListenerRequest(getName(), null, null);
        EventHandler<ReplicatedMapPortableEntryEvent> handler = createHandler(listener);
        return listenOnPartitionId(request, getOrInitTargetPartitionId(), handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, K key) {
        ClientReplicatedMapAddEntryListenerRequest request =
                new ClientReplicatedMapAddEntryListenerRequest(getName(), null, key);
        EventHandler<ReplicatedMapPortableEntryEvent> handler = createHandler(listener);
        return listenOnPartitionId(request, getOrInitTargetPartitionId(), handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate) {
        ClientReplicatedMapAddEntryListenerRequest request =
                new ClientReplicatedMapAddEntryListenerRequest(getName(), predicate, null);
        EventHandler<ReplicatedMapPortableEntryEvent> handler = createHandler(listener);
        return listenOnPartitionId(request, getOrInitTargetPartitionId(), handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key) {
        ClientRequest request = new ClientReplicatedMapAddEntryListenerRequest(getName(), predicate, key);
        EventHandler<ReplicatedMapPortableEntryEvent> handler = createHandler(listener);
        return listenOnPartitionId(request, getOrInitTargetPartitionId(), handler);
    }

    @Override
    public Set<K> keySet() {
        final ClientReplicatedMapKeySetRequest request = new ClientReplicatedMapKeySetRequest(getName());
        final ReplicatedMapKeySet response = invokeOnPartition(request, getOrInitTargetPartitionId());
        return response.getKeySet();
    }

    @Override
    public Collection<V> values() {
        final ClientReplicatedMapValuesRequest request = new ClientReplicatedMapValuesRequest(getName());
        final ReplicatedMapValueCollection response = invokeOnPartition(request, getOrInitTargetPartitionId());
        return response.getValues();
    }

    @Override
    public Collection<V> values(Comparator<V> comparator) {
        List values = (List) values();
        Collections.sort(values, comparator);
        return values;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        final ClientReplicatedMapEntrySetRequest request = new ClientReplicatedMapEntrySetRequest(getName());
        final ReplicatedMapEntrySet response = invokeOnPartition(request, getOrInitTargetPartitionId());
        return response.getEntrySet();
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
            ClientHeapNearCache<Object> nearCache = new ClientHeapNearCache<Object>(getName(),
                    getContext(), nearCacheConfig);
            this.nearCache = nearCache;
            if (nearCache.isInvalidateOnChange()) {
                addNearCacheInvalidateListener();
            }
        }
    }

    private void addNearCacheInvalidateListener() {
        try {
            ClientRequest request = new ClientReplicatedMapAddNearCacheListenerRequest(getName());
            EventHandler handler = new ReplicatedMapNearCacheEventHandler();

            String registrationId = getContext().getListenerService().startListening(request, getName(), handler);
            nearCache.setId(registrationId);
        } catch (Exception e) {
            Logger.getLogger(ClientHeapNearCache.class).severe(
                    "-----------------\n Near Cache is not initialized!!! \n-----------------", e);
        }
    }

    private void removeNearCacheInvalidationListener() {
        if (nearCache != null && nearCache.getId() != null) {
            String registrationId = nearCache.getId();
            BaseClientRemoveListenerRequest request =
                    new ClientReplicatedMapRemoveEntryListenerRequest(getName(), registrationId);
            getContext().getListenerService().stopListeningOnPartition(request, registrationId, getOrInitTargetPartitionId());
        }
    }

    @Override
    public String toString() {
        return "ReplicatedMap{" + "name='" + getName() + '\'' + '}';
    }

    private class ReplicatedMapEventHandler implements EventHandler<ReplicatedMapPortableEntryEvent> {
        private final EntryListener<K, V> listener;

        public ReplicatedMapEventHandler(EntryListener<K, V> listener) {
            this.listener = listener;
        }

        public void handle(ReplicatedMapPortableEntryEvent event) {
            V value = (V) event.getValue();
            V oldValue = (V) event.getOldValue();
            K key = (K) event.getKey();
            Member member = getContext().getClusterService().getMember(event.getUuid());
            EntryEvent<K, V> entryEvent = new EntryEvent<K, V>(getName(), member, event.getEventType().getType(), key,
                    oldValue, value);
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

    private class ReplicatedMapNearCacheEventHandler implements EventHandler<ReplicatedMapPortableEntryEvent> {
        @Override
        public void handle(ReplicatedMapPortableEntryEvent event) {
            switch (event.getEventType()) {
                case ADDED:
                case REMOVED:
                case UPDATED:
                case EVICTED:
                    nearCache.remove(event.getKey());
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

}
