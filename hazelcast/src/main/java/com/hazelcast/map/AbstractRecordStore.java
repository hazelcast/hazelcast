package com.hazelcast.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.Clock;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Contains record store common parts.
 */
abstract class AbstractRecordStore implements RecordStore {

    protected final ConcurrentMap<Data, Record> records = new ConcurrentHashMap<Data, Record>(1000);
    protected final RecordFactory recordFactory;
    protected final String name;
    protected final MapContainer mapContainer;
    protected final MapServiceContext mapServiceContext;

    protected AbstractRecordStore(MapContainer mapContainer) {
        this.mapContainer = mapContainer;
        this.mapServiceContext = mapContainer.getMapServiceContext();
        this.name = mapContainer.getName();
        this.recordFactory = mapContainer.getRecordFactory();
    }

    protected void clearRecordsMap(Map<Data, Record> excludeRecords) {
        InMemoryFormat inMemoryFormat = recordFactory.getStorageFormat();
        switch (inMemoryFormat) {
            case BINARY:
            case OBJECT:
                records.clear();
                if (excludeRecords != null && !excludeRecords.isEmpty()) {
                    records.putAll(excludeRecords);
                }
                return;

            case OFFHEAP:
                Iterator<Record> iter = records.values().iterator();
                while (iter.hasNext()) {
                    Record record = iter.next();
                    if (excludeRecords == null || !excludeRecords.containsKey(record.getKey())) {
                        record.invalidate();
                        iter.remove();
                    }
                }
                return;

            default:
                throw new IllegalArgumentException("Unknown storage format: " + inMemoryFormat);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public MapContainer getMapContainer() {
        return mapContainer;
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }
}
