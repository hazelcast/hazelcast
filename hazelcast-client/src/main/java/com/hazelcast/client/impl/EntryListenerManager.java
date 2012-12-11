/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl;

import com.hazelcast.client.Call;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.Packet;
import com.hazelcast.client.ProxyHelper;
import com.hazelcast.core.EntryListener;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.impl.DataAwareEntryEvent;
import com.hazelcast.impl.Keys;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.serialization.SerializerRegistry;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.nio.IOUtil.toByteArray;
import static com.hazelcast.nio.IOUtil.toObject;

public class EntryListenerManager {

    private final SerializerRegistry serializerRegistry;
    private final Object NULL_KEY = new Object();

    private final ConcurrentMap<String, ConcurrentHashMap<Object, List<EntryListenerHolder>>> entryListeners =
            new ConcurrentHashMap<String, ConcurrentHashMap<Object, List<EntryListenerHolder>>>();

    public EntryListenerManager(final SerializerRegistry serializerRegistry) {this.serializerRegistry
            = serializerRegistry;}

    public synchronized void registerListener(String name, Object key, boolean includeValue, EntryListener<?, ?> entryListener) {
        ConcurrentHashMap<Object, List<EntryListenerHolder>> map = entryListeners.get(name);
        key = toKey(key);
        if (map == null) {
            map = new ConcurrentHashMap<Object, List<EntryListenerHolder>>();
            final ConcurrentHashMap<Object, List<EntryListenerHolder>> map2 = entryListeners.putIfAbsent(name, map);
            if (map2 != null) {
                map = map2;
            }
        }
        if (!map.contains(key)) {
            map.putIfAbsent(key, new CopyOnWriteArrayList<EntryListenerHolder>());
        }
        map.get(key).add(new EntryListenerHolder(entryListener, includeValue));
    }

    private Object toKey(final Object key) {
        return key != null ? key : NULL_KEY;
    }

    private Object fromKey(final Object key) {
        return key != NULL_KEY ? key : null;
    }

    public synchronized void removeListener(String name, Object key, EntryListener<?, ?> entryListener) {
        Map<Object, List<EntryListenerHolder>> m = entryListeners.get(name);
        if (m != null) {
            key = toKey(key);
            List<EntryListenerHolder> list = m.get(key);
            if (list != null) {
                for (final Iterator<EntryListenerHolder> it = list.iterator(); it.hasNext(); ) {
                    final EntryListenerHolder entryListenerHolder = it.next();
                    if (entryListenerHolder.listener.equals(entryListener)) {
                        list.remove(entryListenerHolder);
                    }
                }
                if (m.get(key).isEmpty()) {
                    m.remove(key);
                }
            }
            if (m.isEmpty()) {
                entryListeners.remove(name);
            }
        }
    }

    public synchronized Boolean noListenerRegistered(Object key, String name, boolean includeValue) {
        final Map<Object, List<EntryListenerHolder>> map = entryListeners.get(name);
        key = toKey(key);
        if (map == null || map.get(key) == null) {
            return Boolean.TRUE;
        }
        for (final EntryListenerHolder holder : map.get(key)) {
            if (holder.includeValue == includeValue) {
                return Boolean.FALSE;
            } else if (includeValue) {
                return null;
            }
        }
        return Boolean.TRUE;
    }

    public void notifyListeners(Packet packet) {
        Object keyObj = toObject(packet.getKey());
        Object value = toObject(packet.getValue());
        Data newValue = null;
        Data oldValue = null;
        if (value instanceof Keys) {
            final Keys values = (Keys) value;
            final Iterator<Data> it = values.getKeys().iterator();
            newValue = it.hasNext() ? new Data(it.next().buffer) : null;
            oldValue = it.hasNext() ? new Data(it.next().buffer) : null;
        } else {
            newValue = new Data(packet.getValue());
        }
        final DataAwareEntryEvent event = new DataAwareEntryEvent(null,
                (int) packet.getLongValue(),
                packet.getName(),
                new Data(packet.getKey()),
                newValue,
                oldValue,
                true, serializerRegistry);
        String name = packet.getName();
        Object key = toKey(keyObj);
        if (entryListeners.get(name) != null) {
            notifyListeners(event, entryListeners.get(name).get(NULL_KEY));
            if (key != NULL_KEY) {
                notifyListeners(event, entryListeners.get(name).get(key));
            }
        }
    }

    private void notifyListeners(DataAwareEntryEvent event, Collection<EntryListenerHolder> collection) {
        if (collection == null) {
            return;
        }
        DataAwareEntryEvent eventNoValue = event.getValue() != null ?
                new DataAwareEntryEvent(event.getMember(), event.getEventType().getType(), event.getName(),
                                        event.getKeyData(), null, null, false, serializerRegistry) :
                event;
        switch (event.getEventType()) {
            case ADDED:
                for (EntryListenerHolder holder : collection) {
                    holder.listener.entryAdded(holder.includeValue ? event : eventNoValue);
                }
                break;
            case UPDATED:
                for (EntryListenerHolder holder : collection) {
                    holder.listener.entryUpdated(holder.includeValue ? event : eventNoValue);
                }
                break;
            case REMOVED:
                for (EntryListenerHolder holder : collection) {
                    holder.listener.entryRemoved(holder.includeValue ? event : eventNoValue);
                }
                break;
            case EVICTED:
                for (EntryListenerHolder holder : collection) {
                    holder.listener.entryEvicted(holder.includeValue ? event : eventNoValue);
                }
                break;
        }
    }

    public Call createNewAddListenerCall(final ProxyHelper proxyHelper, final Object key, boolean includeValue) {
        Packet request = proxyHelper.createRequestPacket(ClusterOperation.ADD_LISTENER, toByteArray(key), null);
        request.setLongValue(includeValue ? 1 : 0);
        return proxyHelper.createCall(request);
    }

    public Collection<Call> calls(final HazelcastClient client) {
        final List<Call> calls = new ArrayList<Call>();
        for (final Entry<String, ConcurrentHashMap<Object, List<EntryListenerHolder>>> entry : entryListeners.entrySet()) {
            final String name = entry.getKey();
            final ConcurrentHashMap<Object, List<EntryListenerHolder>> value = entry.getValue();
            for (final Entry<Object, List<EntryListenerHolder>> anotherEntry : value.entrySet()) {
                boolean includeValue = false;
                final Object key = fromKey(anotherEntry.getKey());
                for (final EntryListenerHolder entryListenerHolder : anotherEntry.getValue()) {
                    includeValue |= entryListenerHolder.includeValue;
                    if (includeValue) break;
                }
                final ProxyHelper proxyHelper = new ProxyHelper(name, client);
                calls.add(createNewAddListenerCall(proxyHelper, key, includeValue));
            }
        }
        return calls;
    }

    private static class EntryListenerHolder {
        private final EntryListener listener;
        private boolean includeValue;

        public EntryListenerHolder(EntryListener listener, boolean includeValue) {
            this.listener = listener;
            this.includeValue = includeValue;
        }
    }
}
