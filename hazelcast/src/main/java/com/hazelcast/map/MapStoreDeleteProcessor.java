/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.ScheduledEntry;
import com.hazelcast.util.scheduler.ScheduledEntryProcessor;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class MapStoreDeleteProcessor implements ScheduledEntryProcessor<Data, Object> {

    MapContainer mapContainer;
    MapService mapService;

    public MapStoreDeleteProcessor(MapContainer mapContainer, MapService mapService) {
        this.mapContainer = mapContainer;
        this.mapService = mapService;
    }

    public void process(EntryTaskScheduler<Data, Object> scheduler, Collection<ScheduledEntry<Data, Object>> entries) {
        if(entries.isEmpty())
            return;

        if(entries.size() == 1) {
            mapContainer.getStore().delete(mapService.toObject(entries.iterator().next().getKey()));
            return;
        }

        Set keys = new HashSet();
        for (ScheduledEntry<Data, Object> entry : entries) {
            keys.add(mapService.toObject(entry.getKey()));
        }
        mapContainer.getStore().deleteAll(keys);
    }

}
