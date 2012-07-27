/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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
import com.hazelcast.core.*;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.impl.DataAwareEntryEvent;
import com.hazelcast.impl.DataAwareItemEvent;
import com.hazelcast.nio.serialization.SerializerManager;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ItemListenerManager {

    final Map<ItemListener, EntryListener> itemListener2EntryListener = new ConcurrentHashMap<ItemListener, EntryListener>();
    final private EntryListenerManager entryListenerManager;
    final private SerializerManager serializerManager;

    public ItemListenerManager(EntryListenerManager entryListenerManager, final SerializerManager serializerManager) {
        this.entryListenerManager = entryListenerManager;
        this.serializerManager = serializerManager;
    }

    public synchronized <E, V> void registerListener(final String name, final ItemListener<V> itemListener, boolean includeValue) {
        EntryListener<E, V> e = new EntryAdapter<E, V>() {
            public void entryAdded(EntryEvent<E, V> event) {
                DataAwareEntryEvent dataAwareEntryEvent = (DataAwareEntryEvent) event;
                itemListener.itemAdded(new DataAwareItemEvent(name, ItemEventType.ADDED, dataAwareEntryEvent.getNewValueData(),
                        serializerManager));
            }

            public void entryRemoved(EntryEvent<E, V> event) {
                DataAwareEntryEvent dataAwareEntryEvent = (DataAwareEntryEvent) event;
                itemListener.itemRemoved(new DataAwareItemEvent(name, ItemEventType.REMOVED, dataAwareEntryEvent.getNewValueData(),
                        serializerManager));
            }
        };
        entryListenerManager.registerListener(name, null, includeValue, e);
        itemListener2EntryListener.put(itemListener, e);
    }

    public synchronized void removeListener(String name, ItemListener itemListener) {
        EntryListener entryListener = itemListener2EntryListener.remove(itemListener);
        entryListenerManager.removeListener(name, null, entryListener);
    }

    public Call createNewAddListenerCall(final ProxyHelper proxyHelper, boolean includeValue) {
        Packet request = proxyHelper.createRequestPacket(ClusterOperation.ADD_LISTENER, null, null);
        request.setLongValue(includeValue ? 1 : 0);
        return proxyHelper.createCall(request);
    }

    public Collection<Call> calls(final HazelcastClient client) {
        return Collections.emptyList();
    }
}
