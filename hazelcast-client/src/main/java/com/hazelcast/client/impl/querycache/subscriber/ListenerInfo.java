/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Contains all listener specific information for {@link ClientQueryCacheEventService}.
 *
 * @see ClientQueryCacheEventService
 */
public class ListenerInfo {

    private final String id;
    private final EventFilter filter;
    private final ListenerAdapter listenerAdapter;

    public ListenerInfo(EventFilter filter, ListenerAdapter listenerAdapter, String id) {
        this.id = id;
        this.filter = filter;
        this.listenerAdapter = listenerAdapter;
    }

    public EventFilter getFilter() {
        return filter;
    }

    public ListenerAdapter getListenerAdapter() {
        return listenerAdapter;
    }

    public String getId() {
        return id;
    }
}
