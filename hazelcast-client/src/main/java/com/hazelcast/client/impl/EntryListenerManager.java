/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client.impl;

import com.hazelcast.client.Packet;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.client.Serializer.toObject;

public class EntryListenerManager {
    final private Map<String, Map<Object, List<EntryListener<?, ?>>>> entryListeners = new ConcurrentHashMap<String, Map<Object, List<EntryListener<?, ?>>>>();

    public synchronized void registerEntryListener(String name, Object key, EntryListener<?, ?> entryListener) {
        if (!entryListeners.containsKey(name)) {
            entryListeners.put(name, new HashMap<Object, List<EntryListener<?, ?>>>());
        }
        if (!entryListeners.get(name).containsKey(key)) {
            entryListeners.get(name).put(key, new CopyOnWriteArrayList<EntryListener<?, ?>>());
        }
        entryListeners.get(name).get(key).add(entryListener);
    }

    public synchronized void removeEntryListener(String name, Object key, EntryListener<?, ?> entryListener) {
        Map<Object, List<EntryListener<?, ?>>> m = entryListeners.get(name);
        if (m != null) {
            List<EntryListener<?, ?>> list = m.get(key);
            if (list != null) {
                list.remove(entryListener);
                if (m.get(key).size() == 0) {
                    m.remove(key);
                }
            }
            if (m.size() == 0) {
                entryListeners.remove(name);
            }
        }
    }

    public synchronized boolean noEntryListenerRegistered(Object key, String name) {
        return !(entryListeners.get(name) != null &&
                entryListeners.get(name).get(key) != null &&
                entryListeners.get(name).get(key).size() > 0);
    }

    public void notifyEntryListeners(Packet packet) {
        EntryEvent event = new EntryEvent(packet.getName(), null, (int) packet.getLongValue(), toObject(packet.getKey()), toObject(packet.getValue()));
        String name = event.getName();
        Object key = event.getKey();
        if (entryListeners.get(name) != null) {
            notifyEntryListeners(event, entryListeners.get(name).get(null));
            notifyEntryListeners(event, entryListeners.get(name).get(key));
        }
    }

    private void notifyEntryListeners(EntryEvent event, Collection<EntryListener<?, ?>> collection) {
        if (collection == null) {
            return;
        }
        switch (event.getEventType()) {
            case ADDED:
                for (EntryListener<?, ?> entryListener : collection) {
                    entryListener.entryAdded(event);
                }
                break;
            case UPDATED:
                for (EntryListener<?, ?> entryListener : collection) {
                    entryListener.entryUpdated(event);
                }
                break;
            case REMOVED:
                for (EntryListener<?, ?> entryListener : collection) {
                    entryListener.entryRemoved(event);
                }
                break;
            case EVICTED:
                for (EntryListener<?, ?> entryListener : collection) {
                    entryListener.entryEvicted(event);
                }
                break;
        }
    }
}
