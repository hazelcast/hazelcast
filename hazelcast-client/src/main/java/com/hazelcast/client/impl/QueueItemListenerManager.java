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
import com.hazelcast.client.ProxyHelper;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.ItemListener;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationConstants;
import com.hazelcast.nio.serialization.SerializationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class QueueItemListenerManager {
    final private ConcurrentHashMap<String, List<ItemListenerHolder>> queueItemListeners = new ConcurrentHashMap<String, List<ItemListenerHolder>>();
    final private SerializationService serializerRegistry;

    public QueueItemListenerManager(final SerializationService serializerRegistry) {
        this.serializerRegistry = serializerRegistry;
    }

    public Collection<? extends Call> calls(HazelcastClient client) {
        final List<Call> calls = new ArrayList<Call>();
        for (final String name : queueItemListeners.keySet()) {
            final ProxyHelper proxyHelper = new ProxyHelper(name, client);
            calls.add(createNewAddItemListenerCall(proxyHelper, true));
        }
        return calls;
    }

    public Call createNewAddItemListenerCall(ProxyHelper proxyHelper, boolean includeValue) {
//        Packet request = proxyHelper.createRequestPacket(ClusterOperation.ADD_LISTENER, null, null);
//        request.setLongValue(includeValue ? 1 : 0);
//        return proxyHelper.createCall(request);
        return null;
    }
//    public void notifyListeners(Packet packet) {
//        List<ItemListenerHolder> list = queueItemListeners.get(packet.getName());
//        if (list != null) {
//            for (ItemListenerHolder listenerHolder : list) {
//                ItemListener<Object> listener = listenerHolder.listener;
//                Boolean added = (Boolean) toObject(packet.getValue());
//                if (added) {
//                    listener.itemAdded(new DataAwareItemEvent(packet.getName(), ItemEventType.ADDED,
//                            listenerHolder.includeValue ? new Data(packet.getKey()) : null, null, serializerRegistry));
//                } else {
//                    listener.itemRemoved(new DataAwareItemEvent(packet.getName(), ItemEventType.REMOVED,
//                            listenerHolder.includeValue ? new Data(packet.getKey()) : null, null, serializerRegistry));
//                }
//            }
//        }
//    }

    public void notifyListeners(Protocol protocol) {
        String name = protocol.args[0];
        List<ItemListenerHolder> list = queueItemListeners.get(name);
        if (list != null) {
            for (ItemListenerHolder listenerHolder : list) {
                ItemListener<Object> listener = listenerHolder.listener;
                Data item = listenerHolder.includeValue ? new Data(SerializationConstants.CONSTANT_TYPE_BYTE_ARRAY,
                        protocol.buffers[0].array()) : null;
                ItemEventType itemEventType = ItemEventType.valueOf(protocol.args[1]);
                if (ItemEventType.ADDED.equals(itemEventType)) {
                    listener.itemAdded(new DataAwareItemEvent(name, ItemEventType.ADDED, item, null, serializerRegistry));
                } else {
                    listener.itemRemoved(new DataAwareItemEvent(name, ItemEventType.REMOVED, item, null, serializerRegistry));
                }
            }
        }
    }

    public <E> void removeListener(String name, ItemListener<E> listener) {
        if (!queueItemListeners.containsKey(name)) {
            return;
        }
        queueItemListeners.get(name).remove(listener);
        if (queueItemListeners.get(name).isEmpty()) {
            queueItemListeners.remove(name);
        }
    }

    public <E> void registerListener(String name, ItemListener<E> listener, boolean includeValue) {
        List<ItemListenerHolder> newListenersList = new CopyOnWriteArrayList<ItemListenerHolder>();
        List<ItemListenerHolder> listeners = queueItemListeners.putIfAbsent(name, newListenersList);
        if (listeners == null) {
            listeners = newListenersList;
        }
        listeners.add(new ItemListenerHolder(listener, includeValue));
    }

    public boolean noListenerRegistered(String name) {
        if (!queueItemListeners.containsKey(name)) {
            return true;
        }
        return queueItemListeners.get(name).isEmpty();
    }

    private static class ItemListenerHolder {
        private final ItemListener listener;
        private boolean includeValue;

        public ItemListenerHolder(ItemListener listener, boolean includeValue) {
            this.listener = listener;
            this.includeValue = includeValue;
        }
    }
}
