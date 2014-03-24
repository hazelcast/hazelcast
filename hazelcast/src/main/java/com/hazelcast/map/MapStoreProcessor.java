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
        final List<ScheduledEntry<Data, Object>> entriesToProcess = new ArrayList<ScheduledEntry<Data, Object>>();

        ProcessMode mode = null;
        ProcessMode previousMode;
        // process entries by preserving order.
        for (final ScheduledEntry<Data, Object> entry : scheduledEntries) {
            previousMode = mode;
            if (entry.getValue() == null) {
                mode = ProcessMode.DELETE;
            } else {
                mode = ProcessMode.WRITE;
            }
            if (previousMode != null && !previousMode.equals(mode)) {
                doProcess(scheduler, entriesToProcess, previousMode);
                entriesToProcess.clear();
            }
            entriesToProcess.add(entry);
        }

        doProcess(scheduler, entriesToProcess, mode);
        entriesToProcess.clear();
    }


    private void doProcess(EntryTaskScheduler<Data, Object> scheduler, List<ScheduledEntry<Data, Object>> entriesToProcess, ProcessMode mode) {
        switch (mode) {
            case DELETE:
                deleteProcessor.process(scheduler, entriesToProcess);
                break;
            case WRITE:
                writeProcessor.process(scheduler, entriesToProcess);
                break;
            default:
                throw new IllegalArgumentException("Not found any appropriate processor for mode [" + mode + "]");
        }
    }

    private enum ProcessMode {
        DELETE,
        WRITE;
    }


}
