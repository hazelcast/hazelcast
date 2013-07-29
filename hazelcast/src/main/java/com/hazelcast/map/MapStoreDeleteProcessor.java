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

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.ScheduledEntry;
import com.hazelcast.util.scheduler.ScheduledEntryProcessor;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

public class MapStoreDeleteProcessor implements ScheduledEntryProcessor<Data, Object> {

    private final MapContainer mapContainer;
    private final MapService mapService;

    public MapStoreDeleteProcessor(MapContainer mapContainer, MapService mapService) {
        this.mapContainer = mapContainer;
        this.mapService = mapService;
    }

    private Exception tryDelete(EntryTaskScheduler<Data, Object> scheduler, ScheduledEntry<Data, Object> entry) {
        Exception exception = null;
        try {
            mapContainer.getStore().delete(mapService.toObject(entry.getKey()));
        } catch (Exception e) {
            exception = e;
            scheduler.schedule(mapContainer.getWriteDelayMillis(), entry.getKey(), entry.getValue());
        }
        return exception;
    }


    public void process(EntryTaskScheduler<Data, Object> scheduler, Collection<ScheduledEntry<Data, Object>> entries) {
        if (entries.isEmpty())
            return;

        final ILogger logger = mapService.getNodeEngine().getLogger(getClass());
        if (entries.size() == 1) {
            ScheduledEntry<Data, Object> entry = entries.iterator().next();
            Exception e = tryDelete(scheduler, entry);
            if (e != null) {
                logger.severe(e);
            }
        } else {
            Set keys = new HashSet();
            Exception e = null;
            for (ScheduledEntry<Data, Object> entry : entries) {
                keys.add(mapService.toObject(entry.getKey()));
            }
            try {
                mapContainer.getStore().deleteAll(keys);
            } catch (Exception ex) {
                // if delete all throws exception we will try to put delete them one by one.
                for (ScheduledEntry<Data, Object> entry : entries) {
                    Exception temp = tryDelete(scheduler, entry);
                    if (temp != null) {
                        e = temp;
                    }
                }
            }
            if (e != null) {
                logger.severe(e);
            }
        }
    }

}
