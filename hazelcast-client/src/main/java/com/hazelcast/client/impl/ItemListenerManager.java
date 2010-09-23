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

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ItemListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ItemListenerManager {
    final Map<ItemListener, EntryListener> itemListener2EntryListener = new ConcurrentHashMap<ItemListener, EntryListener>();

    final private EntryListenerManager entryListenerManager;

    public ItemListenerManager(EntryListenerManager entryListenerManager) {
        this.entryListenerManager = entryListenerManager;
    }

    public <E, V> void registerItemListener(String name, final ItemListener<E> itemListener) {
        EntryListener<E, V> e = new EntryListener<E, V>() {
            public void entryAdded(EntryEvent<E, V> event) {
                itemListener.itemAdded((E) event.getKey());
            }

            public void entryEvicted(EntryEvent<E, V> event) {
                // TODO Auto-generated method stub
            }

            public void entryRemoved(EntryEvent<E, V> event) {
                itemListener.itemRemoved((E) event.getKey());
            }

            public void entryUpdated(EntryEvent<E, V> event) {
                // TODO Auto-generated method stub
            }
        };
        entryListenerManager.registerEntryListener(name, null, false, e);
        itemListener2EntryListener.put(itemListener, e);
    }

    public void removeItemListener(String name, ItemListener itemListener) {
        EntryListener entryListener = itemListener2EntryListener.remove(itemListener);
        entryListenerManager.removeEntryListener(name, null, entryListener);
    }
}
