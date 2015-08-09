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

import com.hazelcast.client.impl.ClientMessageDecoder;
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
import com.hazelcast.client.nearcache.ClientHeapNearCache;
import com.hazelcast.client.nearcache.ClientNearCache;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ListenerRemoveCodec;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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
        ClientMessage request = ReplicatedMapPutCodec.encodeRequest(getName(), keyData, valueData, timeUnit.toMillis(ttl));
        ClientMessage response = invoke(request);
        ReplicatedMapPutCodec.ResponseParameters result = ReplicatedMapPutCodec.decodeResponse(response);
        return toObject(result.response);

    }

    @Override
    public int size() {
        ClientMessage request = ReplicatedMapSizeCodec.encodeRequest(getName());
        ClientMessage response = invoke(request);
        ReplicatedMapSizeCodec.ResponseParameters result = ReplicatedMapSizeCodec.decodeResponse(response);
        return result.response;
    }

    @Override
    public boolean isEmpty() {
        ClientMessage request = ReplicatedMapIsEmptyCodec.encodeRequest(getName());
        ClientMessage response = invoke(request);
        ReplicatedMapIsEmptyCodec.ResponseParameters result = ReplicatedMapIsEmptyCodec.decodeResponse(response);
        return result.response;
    }

    @Override
    public boolean containsKey(Object key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        ClientMessage request = ReplicatedMapContainsKeyCodec.encodeRequest(getName(), keyData);
        ClientMessage response = invoke(request);
        ReplicatedMapContainsKeyCodec.ResponseParameters result = ReplicatedMapContainsKeyCodec.decodeResponse(response);
        return result.response;
    }

    @Override
    public boolean containsValue(Object value) {
        checkNotNull(value, NULL_KEY_IS_NOT_ALLOWED);
        Data valueData = toData(value);
        ClientMessage request = ReplicatedMapContainsValueCodec.encodeRequest(getName(), valueData);
        ClientMessage response = invoke(request);
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
                if (cached.equals(ClientNearCache.NULL_OBJECT)) {
                    return null;
                }
                return (V) cached;
            }
        }

        Data keyData = toData(key);
        ClientMessage request = ReplicatedMapGetCodec.encodeRequest(getName(), keyData);
        ClientMessage response = invoke(request);

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
        ClientMessage request = ReplicatedMapRemoveCodec.encodeRequest(getName(), keyData);
        ClientMessage response = invoke(request);
        ReplicatedMapRemoveCodec.ResponseParameters result = ReplicatedMapRemoveCodec.decodeResponse(response);
        return toObject(result.response);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        Map<Data, Data> map = new HashMap<Data, Data>();
        for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
            final Data keyData = toData(entry.getKey());
            map.put(keyData, toData(entry.getValue()));
        }

        ClientMessage request = ReplicatedMapPutAllCodec.encodeRequest(getName(), map);
        invoke(request);
    }

    @Override
    public void clear() {
        ClientMessage request = ReplicatedMapClearCodec.encodeRequest(getName());
        invoke(request);
    }

    @Override
    public boolean removeEntryListener(String registrationId) {
        final String name = getName();
        return stopListening(registrationId, new ListenerRemoveCodec() {
            @Override
            public ClientMessage encodeRequest(String realRegistrationId) {
                return ReplicatedMapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
            }

            @Override
            public boolean decodeResponse(ClientMessage clientMessage) {
                return ReplicatedMapRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        });
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener) {
        ClientMessage request = ReplicatedMapAddEntryListenerCodec.encodeRequest(getName());
        EventHandler<ClientMessage> handler = createHandler(listener);
        ClientMessageDecoder responseDecoder = new ClientMessageDecoder() {
            @Override
            public <T> T decodeClientMessage(ClientMessage clientMessage) {
                return (T) ReplicatedMapAddEntryListenerCodec.decodeResponse(clientMessage).response;
            }
        };
        return listen(request, null, handler, responseDecoder);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        ClientMessage request = ReplicatedMapAddEntryListenerToKeyCodec.encodeRequest(getName(), keyData);
        EventHandler<ClientMessage> handler = createHandler(listener);
        ClientMessageDecoder responseDecoder = new ClientMessageDecoder() {
            @Override
            public <T> T decodeClientMessage(ClientMessage clientMessage) {
                return (T) ReplicatedMapAddEntryListenerToKeyCodec.decodeResponse(clientMessage).response;
            }
        };
        return listen(request, keyData, handler, responseDecoder);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate) {
        Data predicateData = toData(predicate);
        ClientMessage request = ReplicatedMapAddEntryListenerWithPredicateCodec.encodeRequest(getName(), predicateData);
        EventHandler<ClientMessage> handler = createHandler(listener);
        ClientMessageDecoder responseDecoder = new ClientMessageDecoder() {
            @Override
            public <T> T decodeClientMessage(ClientMessage clientMessage) {
                return (T) ReplicatedMapAddEntryListenerWithPredicateCodec.decodeResponse(clientMessage).response;
            }
        };
        return listen(request, null, handler, responseDecoder);
    }

    @Override
    public String addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key) {
        checkNotNull(key, NULL_KEY_IS_NOT_ALLOWED);
        Data keyData = toData(key);
        Data predicateData = toData(predicate);
        ClientMessage request =
                ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.encodeRequest(getName(), keyData, predicateData);
        EventHandler<ClientMessage> handler = createHandler(listener);
        ClientMessageDecoder responseDecoder = new ClientMessageDecoder() {
            @Override
            public <T> T decodeClientMessage(ClientMessage clientMessage) {
                return (T) ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.decodeResponse(clientMessage).response;
            }
        };
        return listen(request, keyData, handler, responseDecoder);
    }

    @Override
    public Set<K> keySet() {
        ClientMessage request = ReplicatedMapKeySetCodec.encodeRequest(getName());
        ClientMessage response = invoke(request);
        ReplicatedMapKeySetCodec.ResponseParameters result = ReplicatedMapKeySetCodec.decodeResponse(response);
        Set<K> resultSet = new HashSet<K>(result.list.size());
        for (Data data : result.list) {
            resultSet.add((K) toObject(data));
        }
        return resultSet;
    }

    @Override
    public Collection<V> values() {
        ClientMessage request = ReplicatedMapValuesCodec.encodeRequest(getName());
        ClientMessage response = invoke(request);
        ReplicatedMapValuesCodec.ResponseParameters result = ReplicatedMapValuesCodec.decodeResponse(response);
        Collection<V> resultCollection = new ArrayList<V>(result.list.size());
        for (Data data : result.list) {
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
        ClientMessage request = ReplicatedMapEntrySetCodec.encodeRequest(getName());
        ClientMessage response = invoke(request);
        ReplicatedMapEntrySetCodec.ResponseParameters result = ReplicatedMapEntrySetCodec.decodeResponse(response);
        Set<Entry<K, V>> resultCollection = new HashSet<Entry<K, V>>(result.map.size());
        for (Entry<Data, Data> dataEntry : result.map.entrySet()) {
            K key = toObject(dataEntry.getKey());
            V value = toObject(dataEntry.getValue());
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
            if (nearCache.isInvalidateOnChange()) {
                addNearCacheInvalidateListener();
            }
        }
    }

    private void addNearCacheInvalidateListener() {
        try {
            ClientMessage request = ReplicatedMapAddNearCacheEntryListenerCodec.encodeRequest(getName(), false);
            EventHandler handler = new ReplicatedMapAddNearCacheEventHandler();
            String registrationId = getContext().getListenerService().startListening(request, null, handler,
                    new ClientMessageDecoder() {
                        @Override
                        public <T> T decodeClientMessage(ClientMessage clientMessage) {
                            return (T) ReplicatedMapAddNearCacheEntryListenerCodec.decodeResponse(clientMessage).response;
                        }
                    });
            nearCache.setId(registrationId);
        } catch (Exception e) {
            Logger.getLogger(ClientHeapNearCache.class).severe(
                    "-----------------\n Near Cache is not initialized!!! \n-----------------", e);
        }
    }

    private void removeNearCacheInvalidationListener() {
        if (nearCache != null && nearCache.getId() != null) {
            String registrationId = nearCache.getId();
            final String name = getName();
            getContext().getListenerService().stopListening(registrationId, new ListenerRemoveCodec() {
                @Override
                public ClientMessage encodeRequest(String realRegistrationId) {
                    return ReplicatedMapRemoveEntryListenerCodec.encodeRequest(name, realRegistrationId);
                }

                @Override
                public boolean decodeResponse(ClientMessage clientMessage) {
                    return ReplicatedMapRemoveEntryListenerCodec.decodeResponse(clientMessage).response;
                }
            });
        }
    }

    @Override
    public String toString() {
        return "ReplicatedMap{" + "name='" + getName() + '\'' + '}';
    }

    private class ReplicatedMapEventHandler extends ReplicatedMapAddEntryListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {
        private final EntryListener<K, V> listener;

        public ReplicatedMapEventHandler(EntryListener<K, V> listener) {
            this.listener = listener;
        }

        @Override
        public void handle(Data keyData, Data valueData, Data oldValueData, Data mergingValue,
                           int eventTypeId, String uuid, int numberOfAffectedEntries) {
            V value = toObject(valueData);
            V oldValue = toObject(oldValueData);
            K key = toObject(keyData);
            Member member = getContext().getClusterService().getMember(uuid);
            EntryEventType eventType = EntryEventType.getByType(eventTypeId);
            EntryEvent<K, V> entryEvent = new EntryEvent<K, V>(getName(), member, eventTypeId, key,
                    oldValue, value);
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

    private class ReplicatedMapAddNearCacheEventHandler extends ReplicatedMapAddNearCacheEntryListenerCodec.AbstractEventHandler
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
                default:
                    throw new IllegalArgumentException("Not a known event type " + entryEventType);
            }
        }
    }
}
