/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map.impl.querycache.subscriber;

import com.hazelcast.map.impl.ListenerAdapter;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.UuidUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.CollectionUtil.isEmpty;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * This class includes mappings for cacheId to its listener-info-collection
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

    public UUID addListener(String cacheId, ListenerAdapter listenerAdapter, EventFilter filter) {
        Collection<ListenerInfo> adapters = getOrPutIfAbsent(registrations, cacheId, LISTENER_SET_CONSTRUCTOR);
        UUID id = UuidUtil.newUnsecureUUID();
        ListenerInfo info = new ListenerInfo(filter, listenerAdapter, id);
        adapters.add(info);
        return id;
    }

    public boolean removeListener(String cacheId, UUID listenerId) {
        Collection<ListenerInfo> adapters = getOrPutIfAbsent(registrations, cacheId, LISTENER_SET_CONSTRUCTOR);
        Iterator<ListenerInfo> iterator = adapters.iterator();
        while (iterator.hasNext()) {
            ListenerInfo listenerInfo = iterator.next();
            UUID listenerInfoId = listenerInfo.getId();
            if (listenerInfoId.equals(listenerId)) {
                iterator.remove();
                return true;
            }
        }
        return false;
    }

    /**
     * Removes all associated listener info for the cache represented with the supplied {@code cacheId}
     *
     * @param cacheId ID of the cache
     */
    public void removeAllListeners(String cacheId) {
        registrations.remove(cacheId);
    }

    /**
     * @return {@code true} if this class contains any registered
     *  listener for the cache represented with the supplied {@code cacheId} else returns {@code false}
     */
    public boolean hasListener(String cacheId) {
        Collection<ListenerInfo> listenerInfos = registrations.get(cacheId);
        return !isEmpty(listenerInfos);
    }

    // this method is only used for testing purposes
    public boolean hasAnyQueryCacheRegistered() {
        return !registrations.isEmpty();
    }

    @SuppressWarnings("unchecked")
    Collection<ListenerInfo> getListenerInfos(String cacheId) {
        Collection<ListenerInfo> infos = registrations.get(cacheId);
        return isEmpty(infos) ? Collections.<ListenerInfo>emptySet() : infos;
    }
}
