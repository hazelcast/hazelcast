package com.hazelcast.map;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.ScheduledEntry;
import com.hazelcast.util.scheduler.ScheduledEntryProcessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MapStoreProcessor implements ScheduledEntryProcessor<Data, Object> {

    private final ScheduledEntryProcessor writeProcessor;
    private final ScheduledEntryProcessor deleteProcessor;

    public MapStoreProcessor(MapContainer mapContainer, MapService mapService) {
        this.writeProcessor = new MapStoreWriteProcessor(mapContainer, mapService);
        this.deleteProcessor = new MapStoreDeleteProcessor(mapContainer, mapService);
    }

    @Override
    public void process(EntryTaskScheduler<Data, Object> scheduler, Collection<ScheduledEntry<Data, Object>> scheduledEntries) {
        if (scheduledEntries == null || scheduledEntries.isEmpty()) {
            return;
        }

        final List<ScheduledEntry<Data, Object>> writes = new ArrayList<ScheduledEntry<Data, Object>>();
        final List<ScheduledEntry<Data, Object>> deletes = new ArrayList<ScheduledEntry<Data, Object>>();

        for (ScheduledEntry<Data, Object> entry : scheduledEntries) {
            if (entry.getValue() == null) {
                deletes.add(entry);
            } else {
                writes.add(entry);
            }
        }

        writeProcessor.process(scheduler, writes);
        deleteProcessor.process(scheduler, deletes);

    }
}
