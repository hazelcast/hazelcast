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

package com.hazelcast.client.proxy.listener;

import com.hazelcast.client.impl.DataAwareEntryEvent;
import com.hazelcast.client.proxy.MapClientProxy;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

import java.net.UnknownHostException;

public class EntryEventLRH<K, V> implements ListenerResponseHandler {

    final EntryListener<K, V> listener;
    final K key;
    final MapClientProxy<K, V> proxy;
    final boolean includeValue;

    public EntryEventLRH(EntryListener<K, V> listener, K key, boolean includeValue, MapClientProxy<K, V> proxy) {
        this.listener = listener;
        this.key = key;
        this.proxy = proxy;
        this.includeValue = includeValue;
    }

    public void handleResponse(Protocol response, SerializationService ss) throws UnknownHostException {
        String eventType = response.args[2];
        String name = response.args[1];
        EntryEventType entryEventType = EntryEventType.valueOf(eventType);
        String[] address = response.args[3].split(":");
        Member source = new MemberImpl(new Address(address[0], Integer.valueOf(address[1])), false);
        final Data value = response.buffers.length > 1 ? response.buffers[1] : null;
        final Data oldValue = response.buffers.length > 2 ? response.buffers[2] : null;
        EntryEvent event = new DataAwareEntryEvent(source, entryEventType.getType(), name,
                response.buffers[0], value, oldValue, false, ss);
        switch (entryEventType) {
            case ADDED:
                listener.entryAdded(event);
                break;
            case REMOVED:
                listener.entryRemoved(event);
                break;
            case UPDATED:
                listener.entryUpdated(event);
                break;
            case EVICTED:
                listener.entryEvicted(event);
                break;
        }
    }

    public void onError(Exception e) {
        proxy.addEntryListener(listener, key, includeValue);
    }
}
