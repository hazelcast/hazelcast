/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.client;

import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.SyntheticEventFilter;
import com.hazelcast.spi.EventFilter;

/**
 * Request for adding {@link com.hazelcast.core.EntryListener} for near cache operations.
 */
public class MapAddNearCacheEntryListenerRequest extends MapAddEntryListenerRequest {

    public MapAddNearCacheEntryListenerRequest() {
    }

    public MapAddNearCacheEntryListenerRequest(String name, boolean includeValue) {
        super(name, includeValue);
    }

    @Override
    protected EventFilter getEventFilter() {
        final EventFilter eventFilter = super.getEventFilter();
        return new SyntheticEventFilter(eventFilter);
    }


    @Override
    public int getClassId() {
        return MapPortableHook.ADD_NEAR_CACHE_ENTRY_LISTENER;
    }
}
