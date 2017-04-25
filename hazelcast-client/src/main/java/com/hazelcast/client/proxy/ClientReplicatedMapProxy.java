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
import com.hazelcast.client.spi.ClientContext;
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
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.monitor.LocalReplicatedMapStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.impl.record.ResultSet;
import com.hazelcast.spi.impl.UnmodifiableLazyList;
import com.hazelcast.util.IterationType;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCache.NOT_CACHED;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.sort;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * The replicated map client side proxy implementation proxying all requests to a member node
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ClientReplicatedMapProxy<K, V> extends ClientProxy implements ReplicatedMap<K, V> {

    private static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    private static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";

    private int targetPartitionId;

    private volatile NearCache<K, V> nearCache;
    private volatile String invalidationListenerId;

    public ClientReplicatedMapProxy(String serviceName, String objectName) {
        super(serviceName, objectName);
    }

    @Override
    protected void onInitialize() {
        int partitionCount = getContext().getPartitionService().getPartitionCount();
        targetPartitionId = ThreadLocalRandomProvider.get().nextInt(partitionCount);

        initNearCache();
    }

    private void initNearCache() {
        ClientContext context = getContext();
        NearCacheConfig nearCacheConfig = context.getClientConfig().getNearCacheConfig(name);
        if (nearCacheConfig != null) {
            nearCache = context.getNearCacheManager().getOrCreateNearCache(name, nearCacheConfig);
            if (nearCacheConfig.isInvalidateOnChange()) {
                addNearCacheInvalidateListener();
            }
        }
    }

    @Override
    protected void onDestroy() {
        if (nearCache != null) {
            removeNearCacheInvalidationListener();
            getContext().getNearCacheManager().destroyNearCache(name);
        }
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        try {
            Data valueData = toData(value);
            Data keyData = toData(key);
            ClientMessage request = ReplicatedMapPutCodec.encodeRequest(name, keyData, valueData, timeUnit.toMillis(ttl));
            ClientMessage response = invoke(request, keyData);
            ReplicatedMapPutCodec.ResponseParameters result = ReplicatedMapPutCodec.decodeResponse(response);

            return toObject(result.response);
        } finally {
            invalidate(key);
        }
    }

    @Override
    public int size() {
        ClientMessage request = ReplicatedMapSizeCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request, targetPartitionId);
        ReplicatedMapSizeCodec.ResponseParameters result = ReplicatedMapSizeCodec.decodeResponse(response);
        return result.response;
    }

    @Override
    public boolean isEmpty() {
        ClientMessage request = ReplicatedMapIsEmptyCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request, targetPartitionId);
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
        ClientMessage response = invokeOnPartition(request, targetPartitionId);
        ReplicatedMapContainsValueCodec.ResponseParameters result = ReplicatedMapContainsValueCodec.decodeResponse(response);
        return result.response;
    }

    @Override
    public V get(Object userKey) {
        K key = validateKey(userKey);

        V cachedValue = getCachedValue(key);
        if (cachedValue != NOT_CACHED) {
            return cachedValue;
        }

        try {
            Data keyData = toData(key);
            long reservationId = tryReserveForUpdate(key);
            ClientMessage request = ReplicatedMapGetCodec.encodeRequest(name, keyData);
            ClientMessage response = invoke(request, keyData);

            ReplicatedMapGetCodec.ResponseParameters result = ReplicatedMapGetCodec.decodeResponse(response);
            V value = toObject(result.response);
            tryPublishReserved(key, value, reservationId);
            return value;
        } catch (Throwable t) {
            invalidate(key);
            throw rethrow(t);
        }
    }

    @Override
    public V put(K key, V value) {
        return put(key, value, 0, MILLISECONDS);
    }

    @Override
    public V remove(Object userKey) {
        K key = validateKey(userKey);

        try {
            Data keyData = toData(key);
            ClientMessage request = ReplicatedMapRemoveCodec.encodeRequest(name, keyData);
            ClientMessage response = invoke(request, keyData);
            ReplicatedMapRemoveCodec.ResponseParameters result = ReplicatedMapRemoveCodec.decodeResponse(response);
            return toObject(result.response);
        } finally {
            invalidate(key);
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        try {
            List<Entry<Data, Data>> dataEntries = new ArrayList<Entry<Data, Data>>(map.size());
            for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
                Data keyData = toData(entry.getKey());
                Data valueData = toData(entry.getValue());

                dataEntries.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(keyData, valueData));
            }

            ClientMessage request = ReplicatedMapPutAllCodec.encodeRequest(name, dataEntries);
            invoke(request);
        } finally {
            if (nearCache != null) {
                for (K key : map.keySet()) {
                    invalidate(key);
                }
            }
        }
    }

    @Override
    public void clear() {
        try {
            ClientMessage request = ReplicatedMapClearCodec.encodeRequest(name);
            invoke(request);
        } finally {
            if (nearCache != null) {
                nearCache.clear();
            }
        }
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
        ClientMessage response = invokeOnPartition(request, targetPartitionId);
        ReplicatedMapKeySetCodec.ResponseParameters result = ReplicatedMapKeySetCodec.decodeResponse(response);
        List<Entry<K, V>> keys = new ArrayList<Entry<K, V>>(result.response.size());
        for (Data dataKey : result.response) {
            keys.add(new AbstractMap.SimpleImmutableEntry<K, V>((K) toObject(dataKey), null));
        }
        return new ResultSet(keys, IterationType.KEY);
    }

    @Override
    public LocalReplicatedMapStats getReplicatedMapStats() {
        throw new UnsupportedOperationException("Replicated Map statistics are not available for client!");
    }

    @Override
    public Collection<V> values() {
        ClientMessage request = ReplicatedMapValuesCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request, targetPartitionId);
        ReplicatedMapValuesCodec.ResponseParameters result = ReplicatedMapValuesCodec.decodeResponse(response);
        return new UnmodifiableLazyList<V>(result.response, getSerializationService());
    }

    @Override
    public Collection<V> values(Comparator<V> comparator) {
        List<V> values = (List<V>) values();
        sort(values, comparator);
        return values;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        ClientMessage request = ReplicatedMapEntrySetCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request, targetPartitionId);
        ReplicatedMapEntrySetCodec.ResponseParameters result = ReplicatedMapEntrySetCodec.decodeResponse(response);
        List<Entry<K, V>> entries = new ArrayList<Entry<K, V>>(result.response.size());
        for (Entry<Data, Data> dataEntry : result.response) {
            K key = toObject(dataEntry.getKey());
            V value = toObject(dataEntry.getValue());
            entries.add(new AbstractMap.SimpleImmutableEntry<K, V>(key, value));
        }
        return new ResultSet<K, V>(entries, IterationType.ENTRY);
    }

    private EventHandler<ClientMessage> createHandler(EntryListener<K, V> listener) {
        return new ReplicatedMapEventHandler(listener);
    }

    private void addNearCacheInvalidateListener() {
        try {
            EventHandler handler = new ReplicatedMapAddNearCacheEventHandler();
            invalidationListenerId = registerListener(createNearCacheInvalidationListenerCodec(), handler);
        } catch (Exception e) {
            ILogger logger = getContext().getLoggingService().getLogger(ClientReplicatedMapProxy.class);
            logger.severe("-----------------\nNear Cache is not initialized!\n-----------------", e);
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

    @SuppressWarnings("unchecked")
    private K validateKey(Object key) {
        return checkNotNull((K) key, NULL_KEY_IS_NOT_ALLOWED);
    }

    @SuppressWarnings("unchecked")
    private V getCachedValue(K key) {
        if (nearCache == null) {
            return (V) NOT_CACHED;
        }

        V value = nearCache.get(key);
        if (value == null) {
            return (V) NOT_CACHED;
        }
        if (value == CACHED_AS_NULL) {
            return null;
        }
        return toObject(value);
    }

    private void tryPublishReserved(K key, V value, long reservationId) {
        if (nearCache == null) {
            return;
        }
        if (reservationId != NOT_RESERVED) {
            nearCache.tryPublishReserved(key, value, reservationId, false);
        }
    }

    private long tryReserveForUpdate(K key) {
        if (nearCache == null) {
            return NOT_RESERVED;
        }
        return nearCache.tryReserveForUpdate(key);
    }

    private void invalidate(K key) {
        if (nearCache == null) {
            return;
        }
        nearCache.remove(key);
    }

    private class ReplicatedMapEventHandler
            extends ReplicatedMapAddEntryListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final EntryListener<K, V> listener;

        ReplicatedMapEventHandler(EntryListener<K, V> listener) {
            this.listener = listener;
        }

        @Override
        public void handle(Data keyData, Data valueData, Data oldValueData, Data mergingValue, int eventTypeId, String uuid,
                           int numberOfAffectedEntries) {
            Member member = getContext().getClusterService().getMember(uuid);
            EntryEventType eventType = EntryEventType.getByType(eventTypeId);
            EntryEvent<K, V> entryEvent = new DataAwareEntryEvent<K, V>(member, eventTypeId, name, keyData,
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
                    throw new IllegalArgumentException("Not a known event type: " + eventType);
            }
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }
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
        public void handle(Data dataKey, Data value, Data oldValue, Data mergingValue, int eventType, String uuid,
                           int numberOfAffectedEntries) {
            EntryEventType entryEventType = EntryEventType.getByType(eventType);
            switch (entryEventType) {
                case ADDED:
                case REMOVED:
                case UPDATED:
                case EVICTED:
                    K key = toObject(dataKey);
                    nearCache.remove(key);
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
