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
import com.hazelcast.client.impl.protocol.parameters.BooleanResultParameters;
import com.hazelcast.client.impl.protocol.parameters.DataCollectionResultParameters;
import com.hazelcast.client.impl.protocol.parameters.DataEntryListResultParameters;
import com.hazelcast.client.impl.protocol.parameters.EntryEventParameters;
import com.hazelcast.client.impl.protocol.parameters.GenericResultParameters;
import com.hazelcast.client.impl.protocol.parameters.IntResultParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapAddEntryListenerParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapAddEntryListenerToKeyParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapAddEntryListenerToKeyWithPredicateParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapAddEntryListenerWithPredicateParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapClearParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapContainsKeyParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapContainsValueParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapEntrySetParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapGetParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapIsEmptyParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapKeySetParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapPutAllParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapPutParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapRemoveEntryListenerParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapRemoveParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapSizeParameters;
import com.hazelcast.client.impl.protocol.parameters.ReplicatedMapValuesParameters;
import com.hazelcast.client.nearcache.ClientHeapNearCache;
import com.hazelcast.client.nearcache.ClientNearCache;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;

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

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * The replicated map client side proxy implementation proxying all requests to a member node
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ClientReplicatedMapProxy<K, V>
        extends ClientProxy
        implements ReplicatedMap<K, V> {

    protected static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    protected static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";

    private volatile ClientHeapNearCache<Object> nearCache;
    private final AtomicBoolean nearCacheInitialized = new AtomicBoolean();

    public ClientReplicatedMapProxy(String serviceName, String objectName) {
        super(serviceName, objectName);
    }

    @Override
    protected void onDestroy() {
        if (nearCache != null) {
            nearCache.destroy();
        }
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data valueData = toData(value);
        Data keyData = toData(key);
        ClientMessage request = ReplicatedMapPutParameters.encode(getName(), keyData, valueData, timeUnit.toMillis(ttl));
        ClientMessage response = invoke(request);
        return toObject(response);

    }

    @Override
    public int size() {
        ClientMessage request = ReplicatedMapSizeParameters.encode(getName());
        ClientMessage response = invoke(request);
        IntResultParameters result = IntResultParameters.decode(response);
        return result.result;
    }

    @Override
    public boolean isEmpty() {
        ClientMessage request = ReplicatedMapIsEmptyParameters.encode(getName());
        ClientMessage response = invoke(request);
        BooleanResultParameters result = BooleanResultParameters.decode(response);
        return result.result;
    }

    @Override
    public boolean containsKey(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        ClientMessage request = ReplicatedMapContainsKeyParameters.encode(getName(), keyData);
        ClientMessage response = invoke(request);
        BooleanResultParameters result = BooleanResultParameters.decode(response);
        return result.result;
    }

    @Override
    public boolean containsValue(Object value) {
        checkNotNull(value, NULL_KEY_IS_NOT_ALLOWED);
        Data valueData = toData(value);
        ClientMessage request = ReplicatedMapContainsValueParameters.encode(getName(), valueData);
        ClientMessage response = invoke(request);
        BooleanResultParameters result = BooleanResultParameters.decode(response);
        return result.result;
    }

    @Override
    public V get(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
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

        Data keyData = toData(key);
        ClientMessage request = ReplicatedMapGetParameters.encode(getName(), keyData);
        ClientMessage response = invoke(request);

        GenericResultParameters result = GenericResultParameters.decode(response);

        V value = (V) toObject(result.result);
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
        ClientMessage request = ReplicatedMapRemoveParameters.encode(getName(), keyData);
        ClientMessage response = invoke(request);
        GenericResultParameters result = GenericResultParameters.decode(response);
        return toObject(result.result);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        List<Data> keyList = new ArrayList<Data>(m.size());
        List<Data> valueList = new ArrayList<Data>(m.size());
        for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
            keyList.add(toData(entry.getKey()));
            valueList.add(toData(entry.getValue()));
        }

        ClientMessage request = ReplicatedMapPutAllParameters.encode(getName(), keyList, valueList);
        invoke(request);
    }

    @Override
    public void clear() {
        ClientMessage request = ReplicatedMapClearParameters.encode(getName());
        ClientMessage response = invoke(request);
        invoke(response);
    }

    @Override
    public boolean removeEntryListener(String id) {
        ClientMessage request = ReplicatedMapRemoveEntryListenerParameters.encode(getName(), id);
        return stopListening(request, id);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener) {
        ClientMessage request = ReplicatedMapAddEntryListenerParameters.encode(getName());
        EventHandler<ClientMessage> handler = createHandler(listener);
        return listen(request, null, handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        ClientMessage request = ReplicatedMapAddEntryListenerToKeyParameters.encode(getName(), keyData);
        EventHandler<ClientMessage> handler = createHandler(listener);
        return listen(request, keyData, handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate) {
        Data predicateData = toData(predicate);
        ClientMessage request = ReplicatedMapAddEntryListenerWithPredicateParameters.encode(getName(), predicateData);
        EventHandler<ClientMessage> handler = createHandler(listener);
        return listen(request, null, handler);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        Data predicateData = toData(predicate);
        ClientMessage request =
                ReplicatedMapAddEntryListenerToKeyWithPredicateParameters.encode(getName(), keyData, predicateData);
        EventHandler<ClientMessage> handler = createHandler(listener);
        return listen(request, keyData, handler);
    }

    @Override
    public Set<K> keySet() {
        ClientMessage request = ReplicatedMapKeySetParameters.encode(getName());
        ClientMessage response = invoke(request);
        DataCollectionResultParameters result = DataCollectionResultParameters.decode(response);
        Set<K> resultSet = new HashSet<K>(result.result.size());
        for (Data data : result.result) {
            resultSet.add((K) toObject(data));
        }
        return resultSet;
    }

    @Override
    public Collection<V> values() {
        ClientMessage request = ReplicatedMapValuesParameters.encode(getName());
        ClientMessage response = invoke(request);
        DataCollectionResultParameters result = DataCollectionResultParameters.decode(response);
        Collection<V> resultCollection = new ArrayList<V>(result.result.size());
        for (Data data : result.result) {
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
        ClientMessage request = ReplicatedMapEntrySetParameters.encode(getName());
        ClientMessage response = invoke(request);
        DataEntryListResultParameters result = DataEntryListResultParameters.decode(response);
        int size = result.keys.size();
        Set<Entry<K, V>> resultCollection = new HashSet<Entry<K, V>>(size);
        for (int i = 0; i < size; i++) {
            K key = toObject(result.keys.get(i));
            V value = toObject(result.values.get(i));
            resultCollection.add(new AbstractMap.SimpleImmutableEntry<K, V>(key, value));
        }
        return resultCollection;
    }

    private EventHandler<ClientMessage> createHandler(final EntryListener<K, V> listener) {
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
        }
    }

    @Override
    public String toString() {
        return "ReplicatedMap{" + "name='" + getName() + '\'' + '}';
    }

    private class ReplicatedMapEventHandler implements EventHandler<ClientMessage> {
        private final EntryListener<K, V> listener;

        public ReplicatedMapEventHandler(EntryListener<K, V> listener) {
            this.listener = listener;
        }

        public void handle(ClientMessage clientMessage) {
            EntryEventParameters event = EntryEventParameters.decode(clientMessage);
            V value = toObject(event.value);
            V oldValue = toObject(event.oldValue);
            K key = toObject(event.key);
            Member member = getContext().getClusterService().getMember(event.uuid);
            EntryEvent<K, V> entryEvent = new EntryEvent<K, V>(getName(), member, event.eventType, key,
                    oldValue, value);
            EntryEventType eventType = EntryEventType.getByType(event.eventType);
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
}
