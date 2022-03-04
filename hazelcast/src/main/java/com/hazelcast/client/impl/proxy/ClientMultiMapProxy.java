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

import com.hazelcast.client.impl.clientside.ClientLockReferenceIdGenerator;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MultiMapAddEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapAddEntryListenerToKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapClearCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapContainsEntryCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapContainsKeyCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapContainsValueCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapDeleteCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapEntrySetCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapForceUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapGetCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapIsLockedCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapKeySetCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapLockCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapPutAllCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapPutCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapRemoveEntryCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapRemoveEntryListenerCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapSizeCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapTryLockCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapValueCountCodec;
import com.hazelcast.client.impl.protocol.codec.MultiMapValuesCodec;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientPartitionService;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.map.IMapEvent;
import com.hazelcast.map.MapEvent;
import com.hazelcast.map.impl.DataAwareEntryEvent;
import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.multimap.LocalMultiMapStats;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.UnmodifiableLazyList;
import com.hazelcast.spi.impl.UnmodifiableLazySet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.map.impl.ListenerAdapters.createListenerAdapter;
import static java.lang.Thread.currentThread;

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
    protected static final String NULL_LISTENER_IS_NOT_ALLOWED = "Null listener is not allowed!";

    private ClientLockReferenceIdGenerator lockReferenceIdGenerator;

    public ClientMultiMapProxy(String serviceName, String name, ClientContext context) {
        super(serviceName, name, context);
    }

    @Override
    public CompletionStage<Void> putAllAsync(@Nonnull Map<? extends K, Collection<? extends V>> m) {
        InternalCompletableFuture<Void> future = new InternalCompletableFuture<>();
        Map<Data, Collection<Data>> dataMap = new HashMap<>();

        for (Map.Entry e : m.entrySet()) {
            Collection<Data> dataCollection = CollectionUtil
                    .objectToDataCollection(((Collection<? extends V>) e.getValue()),
                            getSerializationService());

            dataMap.put(toData(e.getKey()), dataCollection);
        }
        putAllInternal(dataMap, future);
        return future;
    }

    @Override
    public CompletionStage<Void> putAllAsync(@Nonnull K key, Collection<? extends V> value) {
        InternalCompletableFuture<Void> future = new InternalCompletableFuture<>();
        Map<Data, Collection<Data>> dataMap = new HashMap<>();

        Collection<Data> dataCollection = CollectionUtil
                .objectToDataCollection(value, getSerializationService());
        dataMap.put(toData(key), dataCollection);
        putAllInternal(dataMap, future);
        return future;
    }


    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity", "checkstyle:methodlength"})
    private void putAllInternal(@Nonnull Map<Data, Collection<Data>> map,
                                @Nonnull InternalCompletableFuture<Void> future) {

        if (map.isEmpty()) {
            future.complete(null);
            return;
        }

        ClientPartitionService partitionService = getContext().getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        Map<Integer, Collection<Map.Entry<Data, Collection<Data>>>> entryMap = new HashMap<>(partitionCount);

        for (Map.Entry<Data, Collection<Data>> entry : map.entrySet()) {
            checkNotNull(entry.getKey(), NULL_KEY_IS_NOT_ALLOWED);
            checkNotNull(entry.getValue(), NULL_VALUE_IS_NOT_ALLOWED);

            Data keyData = entry.getKey();
            int partitionId = partitionService.getPartitionId(keyData);
            Collection<Map.Entry<Data, Collection<Data>>> partition = entryMap.get(partitionId);
            if (partition == null) {
                partition = new ArrayList<>();
                entryMap.put(partitionId, partition);
            }

            partition.add(new AbstractMap.SimpleEntry<>(keyData, entry.getValue()));
        }
        assert entryMap.size() > 0;
        AtomicInteger counter = new AtomicInteger(entryMap.size());
        InternalCompletableFuture<Void> resultFuture = future;
        BiConsumer<ClientMessage, Throwable> callback = (response, t) -> {
            if (t != null) {
                resultFuture.completeExceptionally(t);
            }
            if (counter.decrementAndGet() == 0) {
                if (!resultFuture.isDone()) {
                    resultFuture.complete(null);
                }
            }
        };

        for (Map.Entry<Integer, Collection<Map.Entry<Data, Collection<Data>>>> entry : entryMap.entrySet()) {
            Integer partitionId = entry.getKey();
            // if there is only one entry, consider how we can use MapPutRequest
            // without having to get back the return value

            ClientMessage request = MultiMapPutAllCodec.encodeRequest(name, entry.getValue());
            new ClientInvocation(getClient(), request, getName(), partitionId)
                    .invoke()
                    .whenCompleteAsync(callback);
        }
    }

    @Override
    public boolean put(@Nonnull K key, @Nonnull V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        Data valueData = toData(value);
        ClientMessage request = MultiMapPutCodec.encodeRequest(name, keyData, valueData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        return MultiMapPutCodec.decodeResponse(response);
    }

    @Nonnull
    @Override
    public Collection<V> get(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        ClientMessage request = MultiMapGetCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        return new UnmodifiableLazyList(MultiMapGetCodec.decodeResponse(response), getSerializationService());
    }

    @Override
    public boolean remove(@Nonnull Object key, @Nonnull Object value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        Data valueData = toData(value);
        ClientMessage request = MultiMapRemoveEntryCodec.encodeRequest(name, keyData, valueData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        return MultiMapRemoveEntryCodec.decodeResponse(response);
    }

    @Nonnull
    @Override
    public Collection<V> remove(@Nonnull Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        ClientMessage request = MultiMapRemoveCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        return new UnmodifiableLazyList(MultiMapRemoveCodec.decodeResponse(response), getSerializationService());
    }

    public void delete(@Nonnull Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        ClientMessage request = MultiMapDeleteCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        invoke(request, keyData);
    }

    @Nonnull
    @Override
    public Set<K> localKeySet() {
        throw new UnsupportedOperationException("Locality for client is ambiguous");
    }

    @Nonnull
    @Override
    public Set<K> keySet() {
        ClientMessage request = MultiMapKeySetCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        return (Set<K>) new UnmodifiableLazySet(MultiMapKeySetCodec.decodeResponse(response), getSerializationService());
    }

    @Nonnull
    @Override
    public Collection<V> values() {
        ClientMessage request = MultiMapValuesCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        return new UnmodifiableLazyList(MultiMapValuesCodec.decodeResponse(response), getSerializationService());
    }

    @Nonnull
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        ClientMessage request = MultiMapEntrySetCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        return (Set) new UnmodifiableLazySet(MultiMapEntrySetCodec.decodeResponse(response), getSerializationService());
    }

    @Override
    public boolean containsKey(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        ClientMessage request = MultiMapContainsKeyCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        return MultiMapContainsKeyCodec.decodeResponse(response);
    }

    @Override
    public boolean containsValue(@Nonnull Object value) {
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyValue = toData(value);
        ClientMessage request = MultiMapContainsValueCodec.encodeRequest(name, keyValue);
        ClientMessage response = invoke(request);
        return MultiMapContainsValueCodec.decodeResponse(response);
    }

    @Override
    public boolean containsEntry(@Nonnull K key, @Nonnull V value) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(value, NULL_VALUE_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        Data valueData = toData(value);
        ClientMessage request = MultiMapContainsEntryCodec.encodeRequest(name, keyData, valueData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        return MultiMapContainsEntryCodec.decodeResponse(response);
    }

    @Override
    public int size() {
        ClientMessage request = MultiMapSizeCodec.encodeRequest(name);
        ClientMessage response = invoke(request);
        return MultiMapSizeCodec.decodeResponse(response);
    }

    @Override
    public void clear() {
        ClientMessage request = MultiMapClearCodec.encodeRequest(name);
        invoke(request);
    }

    @Override
    public int valueCount(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        Data keyData = toData(key);
        ClientMessage request = MultiMapValueCountCodec.encodeRequest(name, keyData, ThreadUtil.getThreadId());
        ClientMessage response = invoke(request, keyData);
        return MultiMapValueCountCodec.decodeResponse(response);
    }

    @Nonnull
    @Override
    public UUID addLocalEntryListener(@Nonnull EntryListener<K, V> listener) {
        throw new UnsupportedOperationException("Locality for client is ambiguous");
    }

    @Nonnull
    @Override
    public UUID addLocalEntryListener(@Nonnull EntryListener<K, V> listener, boolean includeValue) {
        throw new UnsupportedOperationException("Locality for client is ambiguous");
    }

    @Nonnull
    @Override
    public UUID addEntryListener(@Nonnull EntryListener<K, V> listener, final boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        ListenerAdapter listenerAdaptor = createListenerAdapter(listener);
        EventHandler<ClientMessage> handler = new ClientMultiMapEventHandler(listenerAdaptor);
        return registerListener(createEntryListenerCodec(includeValue), handler);
    }

    private ListenerMessageCodec createEntryListenerCodec(final boolean includeValue) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return MultiMapAddEntryListenerCodec.encodeRequest(name, includeValue, localOnly);
            }

            @Override
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return MultiMapAddEntryListenerCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return MultiMapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MultiMapRemoveEntryListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    @Override
    public boolean removeEntryListener(@Nonnull UUID registrationId) {
        checkNotNull(registrationId, "Registration ID should not be null!");
        return deregisterListener(registrationId);
    }

    @Nonnull
    @Override
    public UUID addEntryListener(@Nonnull EntryListener<K, V> listener, @Nonnull K key, final boolean includeValue) {
        checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        final Data keyData = toData(key);
        ListenerAdapter listenerAdapter = createListenerAdapter(listener);
        EventHandler<ClientMessage> handler = new ClientMultiMapToKeyEventHandler(listenerAdapter);
        return registerListener(createEntryListenerToKeyCodec(includeValue, keyData), handler);
    }

    private ListenerMessageCodec createEntryListenerToKeyCodec(final boolean includeValue, final Data keyData) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return MultiMapAddEntryListenerToKeyCodec.encodeRequest(name, keyData, includeValue, localOnly);
            }

            @Override
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return MultiMapAddEntryListenerToKeyCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return MultiMapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MultiMapRemoveEntryListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    @Override
    public void lock(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        ClientMessage request = MultiMapLockCodec
                .encodeRequest(name, keyData, ThreadUtil.getThreadId(), -1, lockReferenceIdGenerator.getNextReferenceId());
        invoke(request, keyData);
    }

    @Override
    public void lock(@Nonnull K key, long leaseTime, @Nonnull TimeUnit timeUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        checkNotNull(timeUnit, "Null timeUnit is not allowed!");
        checkPositive("leaseTime", leaseTime);

        final Data keyData = toData(key);
        ClientMessage request = MultiMapLockCodec
                .encodeRequest(name, keyData, ThreadUtil.getThreadId(), getTimeInMillis(leaseTime, timeUnit),
                        lockReferenceIdGenerator.getNextReferenceId());
        invoke(request, keyData);
    }

    @Override
    public boolean isLocked(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        ClientMessage request = MultiMapIsLockedCodec.encodeRequest(name, keyData);
        ClientMessage response = invoke(request, keyData);
        return MultiMapIsLockedCodec.decodeResponse(response);
    }

    @Override
    public boolean tryLock(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        try {
            return tryLock(key, 0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            return false;
        }
    }

    @Override
    public boolean tryLock(@Nonnull K key, long time, @Nullable TimeUnit timeunit) throws InterruptedException {
        return tryLock(key, time, timeunit, Long.MAX_VALUE, null);
    }

    @Override
    public boolean tryLock(@Nonnull K key,
                           long time, @Nullable TimeUnit timeunit,
                           long leaseTime, @Nullable TimeUnit leaseUnit) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        long timeoutInMillis = getTimeInMillis(time, timeunit);
        long leaseTimeInMillis = getTimeInMillis(leaseTime, leaseUnit);

        long threadId = ThreadUtil.getThreadId();
        ClientMessage request = MultiMapTryLockCodec.encodeRequest(name, keyData, threadId, leaseTimeInMillis, timeoutInMillis,
                lockReferenceIdGenerator.getNextReferenceId());
        ClientMessage response = invoke(request, keyData);
        return MultiMapTryLockCodec.decodeResponse(response);
    }

    @Override
    public void unlock(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        ClientMessage request = MultiMapUnlockCodec
                .encodeRequest(name, keyData, ThreadUtil.getThreadId(), lockReferenceIdGenerator.getNextReferenceId());
        invoke(request, keyData);
    }

    @Override
    public void forceUnlock(@Nonnull K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);

        final Data keyData = toData(key);
        ClientMessage request = MultiMapForceUnlockCodec
                .encodeRequest(name, keyData, lockReferenceIdGenerator.getNextReferenceId());
        invoke(request, keyData);
    }

    @Nonnull
    @Override
    public LocalMultiMapStats getLocalMultiMapStats() {
        throw new UnsupportedOperationException("Locality is ambiguous for client!");
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

    private class ClientMultiMapEventHandler extends AbstractClientMultiMapEventHandler {

        private MultiMapAddEntryListenerCodec.AbstractEventHandler handler;

        ClientMultiMapEventHandler(ListenerAdapter listenerAdapter) {
            super(listenerAdapter);
            handler = new MultiMapAddEntryListenerCodec.AbstractEventHandler() {
                @Override
                public void handleEntryEvent(Data key, Data value, Data oldValue, Data mergingValue,
                                             int eventType, UUID uuid, int numberOfAffectedEntries) {
                    ClientMultiMapEventHandler.this.handleEntryEvent(key, value, oldValue,
                            mergingValue, eventType, uuid, numberOfAffectedEntries);
                }
            };
        }

        @Override
        public void handle(ClientMessage event) {
            handler.handle(event);
        }
    }

    private class ClientMultiMapToKeyEventHandler extends AbstractClientMultiMapEventHandler {

        private MultiMapAddEntryListenerToKeyCodec.AbstractEventHandler handler;

        ClientMultiMapToKeyEventHandler(ListenerAdapter listenerAdapter) {
            super(listenerAdapter);
            handler = new MultiMapAddEntryListenerToKeyCodec.AbstractEventHandler() {
                @Override
                public void handleEntryEvent(Data key, Data value, Data oldValue, Data mergingValue,
                                             int eventType, UUID uuid, int numberOfAffectedEntries) {
                    ClientMultiMapToKeyEventHandler.super.handleEntryEvent(key, value, oldValue,
                            mergingValue, eventType, uuid, numberOfAffectedEntries);
                }
            };
        }

        @Override
        public void handle(ClientMessage clientMessage) {
            handler.handle(clientMessage);
        }
    }

    private abstract class AbstractClientMultiMapEventHandler implements EventHandler<ClientMessage> {

        private final ListenerAdapter listenerAdapter;

        AbstractClientMultiMapEventHandler(ListenerAdapter listenerAdapter) {
            this.listenerAdapter = listenerAdapter;
        }

        public void handleEntryEvent(Data key, Data value, Data oldValue, Data mergingValue,
                                     int eventType, UUID uuid, int numberOfAffectedEntries) {
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
    }


}
