/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.proxy;

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
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapEvent;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.LocalReplicatedMapStats;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.spi.impl.UnmodifiableLazyList;
import com.hazelcast.spi.impl.UnmodifiableLazySet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.nearcache.NearCache.CACHED_AS_NULL;
import static com.hazelcast.internal.nearcache.NearCache.NOT_CACHED;
import static com.hazelcast.internal.nearcache.NearCache.UpdateSemantic.READ_UPDATE;
import static com.hazelcast.internal.nearcache.NearCacheRecord.NOT_RESERVED;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
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
    private static final String NULL_TIMEUNIT_IS_NOT_ALLOWED = "Null time unit is not allowed!";
    private static final String NULL_LISTENER_IS_NOT_ALLOWED = "Null listener is not allowed!";
    private static final String NULL_PREDICATE_IS_NOT_ALLOWED = "Null predicate is not allowed!";

    private int targetPartitionId;

    private volatile NearCache<K, V> nearCache;
    private volatile UUID invalidationListenerId;

    public ClientReplicatedMapProxy(String serviceName, String objectName, ClientContext context) {
        super(serviceName, objectName, context);
    }

    @Override
    protected void onInitialize() {
        int partitionCount = getContext().getPartitionService().getPartitionCount();
        targetPartitionId = ThreadLocalRandomProvider.get().nextInt(partitionCount);

        initNearCache();
    }

    private void initNearCache() {
        NearCacheConfig nearCacheConfig = getContext().getClientConfig().getNearCacheConfig(name);
        if (nearCacheConfig != null) {
            if (nearCacheConfig.isSerializeKeys()) {
                throw new InvalidConfigurationException("ReplicatedMap doesn't support serializeKeys option of NearCacheConfig");
            }

            nearCache = getContext().getNearCacheManager(getServiceName()).getOrCreateNearCache(name, nearCacheConfig);
            if (nearCacheConfig.isInvalidateOnChange()) {
                registerInvalidationListener();
            }
        }
    }

    @Override
    protected void postDestroy() {
        try {
            destroyNearCache();
        } finally {
            super.postDestroy();
        }
    }

    @Override
    protected void onShutdown() {
        try {
            destroyNearCache();
        } finally {
            super.onShutdown();
        }
    }

    private void destroyNearCache() {
        if (nearCache != null) {
            removeNearCacheInvalidationListener();
            getContext().getNearCacheManager(getServiceName()).destroyNearCache(name);
        }
    }

    @Override
    public V put(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        checkNotNull(timeUnit, NULL_TIMEUNIT_IS_NOT_ALLOWED);

        try {
            Data valueData = toData(value);
            Data keyData = toData(key);
            ClientMessage request = ReplicatedMapPutCodec.encodeRequest(name, keyData, valueData, timeUnit.toMillis(ttl));
            ClientMessage response = invoke(request, keyData);
            return toObject(ReplicatedMapPutCodec.decodeResponse(response));
        } finally {
            invalidate(key);
        }
    }

    @Override
    public int size() {
        ClientMessage request = ReplicatedMapSizeCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request, targetPartitionId);
        return ReplicatedMapSizeCodec.decodeResponse(response);
    }

    @Override
    public boolean isEmpty() {
        ClientMessage request = ReplicatedMapIsEmptyCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request, targetPartitionId);
        return ReplicatedMapIsEmptyCodec.decodeResponse(response);
    }

    @Override
    public boolean containsKey(@Nonnull Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        ClientMessage request = ReplicatedMapContainsKeyCodec.encodeRequest(name, keyData);
        ClientMessage response = invoke(request, keyData);
        return ReplicatedMapContainsKeyCodec.decodeResponse(response);
    }

    @Override
    public boolean containsValue(@Nonnull Object value) {
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);
        Data valueData = toData(value);
        ClientMessage request = ReplicatedMapContainsValueCodec.encodeRequest(name, valueData);
        ClientMessage response = invokeOnPartition(request, targetPartitionId);
        return ReplicatedMapContainsValueCodec.decodeResponse(response);
    }

    @Override
    public V get(@Nonnull Object userKey) {
        K key = validateKey(userKey);

        V cachedValue = getCachedValue(key);
        if (cachedValue != NOT_CACHED) {
            return cachedValue;
        }

        try {
            Data keyData = toData(key);
            long reservationId = tryReserveForUpdate(key, keyData);
            ClientMessage request = ReplicatedMapGetCodec.encodeRequest(name, keyData);
            ClientMessage response = invoke(request, keyData);

            V value = toObject(ReplicatedMapGetCodec.decodeResponse(response));
            tryPublishReserved(key, value, reservationId);
            return value;
        } catch (Throwable t) {
            invalidate(key);
            throw rethrow(t);
        }
    }

    @Override
    public V put(@Nonnull K key, @Nonnull V value) {
        return put(key, value, 0, MILLISECONDS);
    }

    @Override
    public V remove(@Nonnull Object userKey) {
        K key = validateKey(userKey);

        try {
            Data keyData = toData(key);
            ClientMessage request = ReplicatedMapRemoveCodec.encodeRequest(name, keyData);
            ClientMessage response = invoke(request, keyData);
            return toObject(ReplicatedMapRemoveCodec.decodeResponse(response));
        } finally {
            invalidate(key);
        }
    }

    @Override
    public void putAll(@Nonnull Map<? extends K, ? extends V> map) {
        checkNotNull(map, "Entries cannot be null");
        try {
            List<Entry<Data, Data>> dataEntries = new ArrayList<>(map.size());
            for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
                Data keyData = toData(entry.getKey());
                Data valueData = toData(entry.getValue());

                dataEntries.add(new AbstractMap.SimpleImmutableEntry<>(keyData, valueData));
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
    public boolean removeEntryListener(@Nonnull UUID registrationId) {
        return deregisterListener(registrationId);
    }

    @Nonnull
    @Override
    public UUID addEntryListener(@Nonnull EntryListener<K, V> listener) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        EventHandler<ClientMessage> handler = new ReplicatedMapEventHandler(listener);
        return registerListener(createEntryListenerCodec(), handler);
    }

    private ListenerMessageCodec createEntryListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return ReplicatedMapAddEntryListenerCodec.encodeRequest(name, localOnly);
            }

            @Override
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return ReplicatedMapAddEntryListenerCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return ReplicatedMapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return ReplicatedMapRemoveEntryListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    @Nonnull
    @Override
    public UUID addEntryListener(@Nonnull EntryListener<K, V> listener, @Nullable K key) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        EventHandler<ClientMessage> handler = new ReplicatedMapToKeyEventHandler(listener);
        return key != null
                ? registerListener(createEntryListenerToKeyCodec(keyData), handler)
                : registerListener(createEntryListenerCodec(), handler);
    }

    private ListenerMessageCodec createEntryListenerToKeyCodec(final Data keyData) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return ReplicatedMapAddEntryListenerToKeyCodec.encodeRequest(name, keyData, localOnly);
            }

            @Override
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return ReplicatedMapAddEntryListenerToKeyCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return ReplicatedMapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return ReplicatedMapRemoveEntryListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    @Nonnull
    @Override
    public UUID addEntryListener(@Nonnull EntryListener<K, V> listener, @Nonnull Predicate<K, V> predicate) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);

        final Data predicateData = toData(predicate);
        EventHandler<ClientMessage> handler = new ReplicatedMapWithPredicateEventHandler(listener);
        return registerListener(createEntryListenerWithPredicateCodec(predicateData), handler);
    }

    private ListenerMessageCodec createEntryListenerWithPredicateCodec(final Data predicateData) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return ReplicatedMapAddEntryListenerWithPredicateCodec.encodeRequest(name, predicateData, localOnly);
            }

            @Override
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return ReplicatedMapAddEntryListenerWithPredicateCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return ReplicatedMapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return ReplicatedMapRemoveEntryListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    @Nonnull
    @Override
    public UUID addEntryListener(@Nonnull EntryListener<K, V> listener,
                                 @Nonnull Predicate<K, V> predicate,
                                 @Nullable K key) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(predicate, NULL_PREDICATE_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        final Data predicateData = toData(predicate);
        EventHandler<ClientMessage> handler = new ReplicatedMapToKeyWithPredicateEventHandler(listener);
        return key != null
                ? registerListener(createEntryListenerToKeyWithPredicateCodec(keyData, predicateData), handler)
                : registerListener(createEntryListenerWithPredicateCodec(predicateData), handler);
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
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return ReplicatedMapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return ReplicatedMapRemoveEntryListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public Set<K> keySet() {
        ClientMessage request = ReplicatedMapKeySetCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request, targetPartitionId);
        return (Set<K>) new UnmodifiableLazySet(ReplicatedMapKeySetCodec.decodeResponse(response), getSerializationService());
    }

    @Nonnull
    @Override
    public LocalReplicatedMapStats getReplicatedMapStats() {
        throw new UnsupportedOperationException("Replicated Map statistics are not available for client!");
    }

    @Nonnull
    @Override
    public Collection<V> values() {
        ClientMessage request = ReplicatedMapValuesCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request, targetPartitionId);
        return new UnmodifiableLazyList(ReplicatedMapValuesCodec.decodeResponse(response), getSerializationService());
    }

    @Nonnull
    @Override
    public Collection<V> values(@Nullable Comparator<V> comparator) {
        List<V> values = (List<V>) values();
        sort(values, comparator);
        return values;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public Set<Entry<K, V>> entrySet() {
        ClientMessage request = ReplicatedMapEntrySetCodec.encodeRequest(name);
        ClientMessage response = invokeOnPartition(request, targetPartitionId);
        return (Set) new UnmodifiableLazySet(ReplicatedMapEntrySetCodec.decodeResponse(response), getSerializationService());
    }

    public UUID addNearCacheInvalidationListener(EventHandler handler) {
        return registerListener(createNearCacheInvalidationListenerCodec(), handler);
    }

    private void registerInvalidationListener() {
        try {
            invalidationListenerId = addNearCacheInvalidationListener(new ReplicatedMapAddNearCacheEventHandler());
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
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return ReplicatedMapAddNearCacheEntryListenerCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return ReplicatedMapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return ReplicatedMapRemoveEntryListenerCodec.decodeResponse(clientMessage);
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

    private long tryReserveForUpdate(K key, Data keyData) {
        if (nearCache == null) {
            return NOT_RESERVED;
        }
        return nearCache.tryReserveForUpdate(key, keyData, READ_UPDATE);
    }

    private void invalidate(K key) {
        if (nearCache == null) {
            return;
        }
        nearCache.invalidate(key);
    }

    private class ReplicatedMapToKeyWithPredicateEventHandler extends AbstractReplicatedMapEventHandler {

        private ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.AbstractEventHandler handler;

        ReplicatedMapToKeyWithPredicateEventHandler(EntryListener<K, V> listener) {
            super(listener);
            handler = new ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.AbstractEventHandler() {
                @Override
                public void handleEntryEvent(Data key, Data value, Data oldValue, Data mergingValue,
                                             int eventType, UUID uuid, int numberOfAffectedEntries) {
                    ReplicatedMapToKeyWithPredicateEventHandler.this.handleEntryEvent(key, value, oldValue,
                            mergingValue, eventType, uuid, numberOfAffectedEntries);
                }
            };
        }

        @Override
        public void handle(ClientMessage event) {
            handler.handle(event);
        }
    }

    private class ReplicatedMapWithPredicateEventHandler extends AbstractReplicatedMapEventHandler {

        private ReplicatedMapAddEntryListenerWithPredicateCodec.AbstractEventHandler handler;

        ReplicatedMapWithPredicateEventHandler(EntryListener<K, V> listener) {
            super(listener);
            handler = new ReplicatedMapAddEntryListenerWithPredicateCodec.AbstractEventHandler() {
                @Override
                public void handleEntryEvent(Data key, Data value, Data oldValue, Data mergingValue,
                                             int eventType, UUID uuid, int numberOfAffectedEntries) {
                    ReplicatedMapWithPredicateEventHandler.this.handleEntryEvent(key, value, oldValue,
                            mergingValue, eventType, uuid, numberOfAffectedEntries);
                }
            };
        }

        @Override
        public void handle(ClientMessage event) {
            handler.handle(event);
        }
    }

    private class ReplicatedMapToKeyEventHandler extends AbstractReplicatedMapEventHandler {

        private ReplicatedMapAddEntryListenerToKeyCodec.AbstractEventHandler handler;

        ReplicatedMapToKeyEventHandler(EntryListener<K, V> listener) {
            super(listener);
            handler = new ReplicatedMapAddEntryListenerToKeyCodec.AbstractEventHandler() {
                @Override
                public void handleEntryEvent(Data key, Data value, Data oldValue, Data mergingValue,
                                             int eventType, UUID uuid, int numberOfAffectedEntries) {
                    ReplicatedMapToKeyEventHandler.this.handleEntryEvent(key, value, oldValue, mergingValue,
                            eventType, uuid, numberOfAffectedEntries);
                }
            };
        }

        @Override
        public void handle(ClientMessage event) {
            handler.handle(event);
        }
    }

    private class ReplicatedMapEventHandler extends AbstractReplicatedMapEventHandler {

        private ReplicatedMapAddEntryListenerCodec.AbstractEventHandler handler;

        ReplicatedMapEventHandler(EntryListener<K, V> listener) {
            super(listener);
            handler = new ReplicatedMapAddEntryListenerCodec.AbstractEventHandler() {
                @Override
                public void handleEntryEvent(Data key, Data value, Data oldValue, Data mergingValue,
                                             int eventType, UUID uuid, int numberOfAffectedEntries) {
                    ReplicatedMapEventHandler.this.handleEntryEvent(key, value, oldValue, mergingValue,
                            eventType, uuid, numberOfAffectedEntries);
                }
            };
        }

        @Override
        public void handle(ClientMessage event) {
            handler.handle(event);
        }
    }

    private abstract class AbstractReplicatedMapEventHandler implements EventHandler<ClientMessage> {

        private final EntryListener<K, V> listener;

        AbstractReplicatedMapEventHandler(EntryListener<K, V> listener) {
            this.listener = listener;
        }

        public void handleEntryEvent(Data keyData, Data valueData, Data oldValueData, Data mergingValue,
                                     int eventTypeId, UUID uuid, int numberOfAffectedEntries) {
            Member member = getContext().getClusterService().getMember(uuid);
            EntryEventType eventType = EntryEventType.getByType(eventTypeId);
            EntryEvent<K, V> entryEvent = new DataAwareEntryEvent<>(member, eventTypeId, name, keyData, valueData,
                    oldValueData, null, getSerializationService());
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
    }

    private class ReplicatedMapAddNearCacheEventHandler
            extends ReplicatedMapAddNearCacheEntryListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        @Override
        public void beforeListenerRegister(Connection connection) {
            if (nearCache != null) {
                nearCache.clear();
            }
        }

        @Override
        public void onListenerRegister(Connection connection) {
            if (nearCache != null) {
                nearCache.clear();
            }
        }

        @Override
        public void handleEntryEvent(Data dataKey, Data value, Data oldValue, Data mergingValue,
                                     int eventType, UUID uuid, int numberOfAffectedEntries) {
            EntryEventType entryEventType = EntryEventType.getByType(eventType);
            switch (entryEventType) {
                case ADDED:
                case REMOVED:
                case UPDATED:
                case EVICTED:
                    K key = toObject(dataKey);
                    nearCache.invalidate(key);
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
