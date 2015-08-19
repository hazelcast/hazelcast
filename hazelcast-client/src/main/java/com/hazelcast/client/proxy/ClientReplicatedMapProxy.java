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
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapEntrySet;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapKeySet;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapPortableEntryEvent;
import com.hazelcast.replicatedmap.impl.client.ReplicatedMapValueCollection;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The replicated map client side proxy implementation proxying all requests to a member node
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ClientReplicatedMapProxy<K, V> extends ClientProxy implements ReplicatedMap<K, V> {

    private volatile ClientHeapNearCache<Object> nearCache;
    private final AtomicBoolean nearCacheInitialized = new AtomicBoolean();

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
        return invoke(new ClientReplicatedMapPutTtlRequest(getName(), dataKey, dataValue, timeUnit.toMillis(ttl)));
    }

    @Override
    public int size() {
        return (Integer) invoke(new ClientReplicatedMapSizeRequest(getName()));
    }

    @Override
    public boolean isEmpty() {
        return (Boolean) invoke(new ClientReplicatedMapIsEmptyRequest(getName()));
    }

    @Override
    public boolean containsKey(Object key) {
        Data dataKey = toData(key);
        return (Boolean) invoke(new ClientReplicatedMapContainsKeyRequest(getName(), dataKey));
    }

    @Override
    public boolean containsValue(Object value) {
        Data dataValue = toData(value);
        return (Boolean) invoke(new ClientReplicatedMapContainsValueRequest(getName(), dataValue));
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
        Data dataKey = toData(key);
        Object response = invoke(new ClientReplicatedMapGetRequest(getName(), dataKey));
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
        return invoke(new ClientReplicatedMapRemoveRequest(getName(), dataKey));
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        Set<? extends Entry<? extends K, ? extends V>> entries = m.entrySet();
        Set<Entry<Data, Data>> dataEntries = new HashSet<Entry<Data, Data>>(m.size());
        for (Entry<? extends K, ? extends V> entry : entries) {
            dataEntries.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(toData(entry.getKey()), toData(entry.getValue())));
        }
        invoke(new ClientReplicatedMapPutAllRequest(getName(), new ReplicatedMapEntrySet(dataEntries)));
    }

    @Override
    public void clear() {
        ClientReplicatedMapClearRequest request = new ClientReplicatedMapClearRequest(getName());
        invoke(request);
    }

    @Override
    public boolean removeEntryListener(String id) {
        final ClientReplicatedMapRemoveEntryListenerRequest request =
                new ClientReplicatedMapRemoveEntryListenerRequest(getName(), id);
        return stopListening(request, id);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener) {
        ClientReplicatedMapAddEntryListenerRequest request = new ClientReplicatedMapAddEntryListenerRequest(getName()
                , null, null);
        EventHandler<ReplicatedMapPortableEntryEvent> handler = createHandler(listener);
        return listen(request, null, handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, K key) {
        Data dataKey = toData(key);
        ClientReplicatedMapAddEntryListenerRequest request = new ClientReplicatedMapAddEntryListenerRequest(getName()
                , null, dataKey);
        EventHandler<ReplicatedMapPortableEntryEvent> handler = createHandler(listener);
        return listen(request, null, handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate) {
        ClientReplicatedMapAddEntryListenerRequest request = new ClientReplicatedMapAddEntryListenerRequest(getName()
                , predicate, null);
        EventHandler<ReplicatedMapPortableEntryEvent> handler = createHandler(listener);
        return listen(request, null, handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key) {
        Data dataKey = toData(key);
        ClientRequest request = new ClientReplicatedMapAddEntryListenerRequest(getName(), predicate, dataKey);
        EventHandler<ReplicatedMapPortableEntryEvent> handler = createHandler(listener);
        return listen(request, null, handler);
    }

    @Override
    public Set<K> keySet() {
        ReplicatedMapKeySet result = invoke(new ClientReplicatedMapKeySetRequest(getName()));
        Set<Data> dataKeyset = result.getKeySet();
        HashSet<K> keySet = new HashSet<K>(dataKeyset.size());
        for (Data dataKey : dataKeyset) {
            keySet.add((K) toObject(dataKey));
        }
        return keySet;
    }

    @Override
    public Collection<V> values() {
        ReplicatedMapValueCollection result = invoke(new ClientReplicatedMapValuesRequest(getName()));
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
        ReplicatedMapEntrySet result = invoke(new ClientReplicatedMapEntrySetRequest(getName()));
        Set<Entry<Data, Data>> dataEntries = result.getEntrySet();
        HashSet<Entry<K, V>> entries = new HashSet<Entry<K, V>>(dataEntries.size());
        for (Entry<Data, Data> dataEntry : dataEntries) {
            K key = toObject(dataEntry.getKey());
            V value = toObject(dataEntry.getValue());
            entries.add(new AbstractMap.SimpleImmutableEntry<K, V>(key, value));
        }
        return entries;
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
            ClientHeapNearCache<Object> nearCache = new ClientHeapNearCache<Object>(getName(), getContext(), nearCacheConfig);
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

            String registrationId = getContext().getListenerService().startListening(request, null, handler);
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
            getContext().getListenerService().stopListening(request, registrationId);
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
            V value = toObject(event.getValue());
            V oldValue = toObject(event.getOldValue());
            K key = toObject(event.getKey());
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
                    nearCache.remove(toObject(event.getKey()));
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
