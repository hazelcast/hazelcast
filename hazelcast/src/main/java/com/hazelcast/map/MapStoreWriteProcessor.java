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

import java.util.*;

public class MapStoreWriteProcessor implements ScheduledEntryProcessor<Data, Object> {

    private final MapContainer mapContainer;
    private final MapService mapService;

    public MapStoreWriteProcessor(MapContainer mapContainer, MapService mapService) {
        this.mapContainer = mapContainer;
        this.mapService = mapService;
    }

    private Exception tryStore(EntryTaskScheduler<Data, Object> scheduler, ScheduledEntry<Data, Object> entry) {
        Exception exception = null;
        try {
            mapContainer.getStore().store(mapService.toObject(entry.getKey()), mapService.toObject(entry.getValue()));
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
            Exception exception = tryStore(scheduler, entry);
            if (exception != null) {
                logger.severe(exception);
            }
        } else {   // if entries size > 0, we will call storeAll
            final Queue<ScheduledEntry> duplicateKeys = new LinkedList<ScheduledEntry>();
            final Map map = new HashMap(entries.size());
            for (ScheduledEntry<Data, Object> entry : entries) {
                final Object key = mapService.toObject(entry.getKey());
                if (map.get(key) != null) {
                    duplicateKeys.offer(entry);
                    continue;
                }
                map.put(key, mapService.toObject(entry.getValue()));
            }
            Exception exception = null;
            try {
                mapContainer.getStore().storeAll(map);
            } catch (Exception e) {
                // if store all throws exception we will try to put insert them one by one.
                for (ScheduledEntry<Data, Object> entry : entries) {
                    Exception temp = tryStore(scheduler, entry);
                    if (temp != null) {
                        exception = temp;
                    }
                }
            }
            ScheduledEntry entry;
            while ((entry = duplicateKeys.poll()) != null) {
                final Exception temp = tryStore(scheduler, entry);
                if (temp != null) {
                    exception = temp;
                }
            }
            if (exception != null) {
                logger.severe(exception);
            }
        }

    }

}
