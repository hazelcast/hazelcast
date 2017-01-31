/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.querycache.subscriber;

import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.UuidUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.CollectionUtil.isEmpty;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * This class includes mappings of {@code cacheName --to--> listener collections}.
 */
public class QueryCacheToListenerMapper {

    private static final ConstructorFunction<String, Collection<ListenerInfo>> LISTENER_SET_CONSTRUCTOR
            = new ConstructorFunction<String, Collection<ListenerInfo>>() {
        @Override
        public Collection<ListenerInfo> createNew(String arg) {
            return Collections.newSetFromMap(new ConcurrentHashMap<ListenerInfo, Boolean>());
        }
    };

    private final ConcurrentMap<String, Collection<ListenerInfo>> registrations;

    QueryCacheToListenerMapper() {
        this.registrations = new ConcurrentHashMap<String, Collection<ListenerInfo>>();
    }

    public String addListener(String cacheName, ListenerAdapter listenerAdapter, EventFilter filter) {
        Collection<ListenerInfo> adapters = getOrPutIfAbsent(registrations, cacheName, LISTENER_SET_CONSTRUCTOR);
        String id = UuidUtil.newUnsecureUuidString();
        ListenerInfo info = new ListenerInfo(filter, listenerAdapter, id);
        adapters.add(info);
        return id;
    }

    public boolean removeListener(String cacheName, String id) {
        Collection<ListenerInfo> adapters = getOrPutIfAbsent(registrations, cacheName, LISTENER_SET_CONSTRUCTOR);
        Iterator<ListenerInfo> iterator = adapters.iterator();
        while (iterator.hasNext()) {
            ListenerInfo listenerInfo = iterator.next();
            String listenerInfoId = listenerInfo.getId();
            if (listenerInfoId.equals(id)) {
                iterator.remove();
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    Collection<ListenerInfo> getListenerInfos(String cacheName) {
        Collection<ListenerInfo> infos = registrations.get(cacheName);
        return isEmpty(infos) ? Collections.EMPTY_SET : infos;
    }
}
