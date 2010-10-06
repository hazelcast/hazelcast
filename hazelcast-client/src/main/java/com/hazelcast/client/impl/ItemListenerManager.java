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

import com.hazelcast.client.Call;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.Packet;
import com.hazelcast.client.ProxyHelper;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ItemListener;
import com.hazelcast.impl.ClusterOperation;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ItemListenerManager {
    final Map<ItemListener, EntryListener> itemListener2EntryListener = new ConcurrentHashMap<ItemListener, EntryListener>();

    final private EntryListenerManager entryListenerManager;

    public ItemListenerManager(EntryListenerManager entryListenerManager) {
        this.entryListenerManager = entryListenerManager;
    }

    public <E, V> void registerItemListener(String name, final ItemListener<E> itemListener) {
        EntryListener<E, V> e = new EntryAdapter<E, V>() {
            public void entryAdded(EntryEvent<E, V> event) {
                itemListener.itemAdded(event.getKey());
            }

            public void entryRemoved(EntryEvent<E, V> event) {
                itemListener.itemRemoved(event.getKey());
            }

        };
        entryListenerManager.registerEntryListener(name, null, false, e);
        itemListener2EntryListener.put(itemListener, e);
    }

    public void removeItemListener(String name, ItemListener itemListener) {
        EntryListener entryListener = itemListener2EntryListener.remove(itemListener);
        entryListenerManager.removeEntryListener(name, null, entryListener);
    }
    
    public Call createNewAddListenerCall(final ProxyHelper proxyHelper, boolean includeValue){
        Packet request = proxyHelper.createRequestPacket(ClusterOperation.ADD_LISTENER, null, null);
        // request.setLongValue(includeValue ? 1 : 0);
        // no make sense to have value for collection
        request.setLongValue(0);
        return proxyHelper.createCall(request);
    }
    
    public Collection<Call> calls(final HazelcastClient client){
    	/*/
        final List<Call> calls = new ArrayList<Call>();
        for (final Entry<String, AtomicInteger> entry : listeners.entrySet()) {
            final String name = entry.getKey();
            final AtomicInteger counter = entry.getValue();
            if (counter.get() > 0){
                final ProxyHelper proxyHelper = new ProxyHelper(name, client);
                calls.add(createNewAddListenerCall(proxyHelper, false));
            }
        }
        return calls;
        /*/
    	// entryListenerManager manages listeners
    	return Collections.emptyList();
    	//*/
    }
}
