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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.client.Serializer.toObject;

public class EntryListenerManager {
    
    private final Object NULL_KEY = new Object();
    
    private final ConcurrentMap<String, ConcurrentHashMap<Object, List<EntryListenerHolder>>> entryListeners =
        new ConcurrentHashMap<String, ConcurrentHashMap<Object, List<EntryListenerHolder>>>();

    public synchronized void registerEntryListener(String name, Object key, boolean includeValue, EntryListener<?, ?> entryListener) {
        ConcurrentHashMap<Object, List<EntryListenerHolder>> map = entryListeners.get(name);
        key = toKey(key);
        if (map == null) {
            map = new ConcurrentHashMap<Object, List<EntryListenerHolder>>();
            final ConcurrentHashMap<Object, List<EntryListenerHolder>> map2 = entryListeners.putIfAbsent(name, map);
            if (map2 != null){
                map = map2;
            }
        }
        if (!map.contains(key)){
            map.putIfAbsent(key, new CopyOnWriteArrayList<EntryListenerHolder>());
        }
        map.get(key).add(new EntryListenerHolder(entryListener, includeValue));
    }

    private Object toKey(final Object key){
        return key != null ? key : NULL_KEY;
    }
    
    public synchronized void removeEntryListener(String name, Object key, EntryListener<?, ?> entryListener) {
        Map<Object, List<EntryListenerHolder>> m = entryListeners.get(name);
        if (m != null) {
            key = toKey(key);
            List<EntryListenerHolder> list = m.get(key);
            if (list != null) {
                for(final Iterator<EntryListenerHolder> it = list.iterator();it.hasNext();){
                    final EntryListenerHolder entryListenerHolder = it.next();
                    if (entryListenerHolder.listener.equals(entryListener)){
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

    public synchronized Boolean noEntryListenerRegistered(Object key, String name, boolean includeValue) {
        final Map<Object, List<EntryListenerHolder>> map = entryListeners.get(name);
        key = toKey(key);
        if (map == null || map.get(key) == null){
            return Boolean.TRUE;
        }
        for (final EntryListenerHolder holder : map.get(key)) {
            if (holder.includeValue == includeValue){
                return Boolean.FALSE;
            } else if (includeValue) {
                return null;
            }
        }
        return Boolean.TRUE;
    }

    public void notifyEntryListeners(Packet packet) {
        Object oldValue = null;
        Object value = toObject(packet.getValue());
        if (value instanceof CollectionWrapper){
            final CollectionWrapper values = (CollectionWrapper) value;
            final Iterator it = values.getKeys().iterator();
            value = it.hasNext() ? it.next() : null;
            oldValue = it.hasNext() ? it.next() : null;
        } 
        final EntryEvent event = new EntryEvent(packet.getName(), null, (int) packet.getLongValue(),
                toObject(packet.getKey()),
                oldValue, value);
        String name = event.getName();
        Object key = toKey(event.getKey());
        if (entryListeners.get(name) != null) {
            notifyEntryListeners(event, entryListeners.get(name).get(NULL_KEY));
            if (key != NULL_KEY){
                notifyEntryListeners(event, entryListeners.get(name).get(key));
            }
        }
    }

    private void notifyEntryListeners(EntryEvent event, Collection<EntryListenerHolder> collection) {
        if (collection == null) {
            return;
        }
        EntryEvent eventNoValue = event.getValue() != null ? 
            new EntryEvent(event.getSource(), event.getMember(), event.getEventType().getType(), event.getKey(), null, null) : 
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
    
    private static class EntryListenerHolder {
        private final EntryListener listener;
        private boolean includeValue;
        
        public EntryListenerHolder(EntryListener listener, boolean includeValue) {
            this.listener = listener;
            this.includeValue = includeValue;
        }
    }
}
